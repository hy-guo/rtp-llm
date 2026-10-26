package org.flexlb.mock;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.flexlb.engine.grpc.EngineRpcService;
import org.flexlb.engine.grpc.RoleTypeProtoConverter;
import org.flexlb.engine.grpc.RpcServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mock implementation of {@link RpcServiceGrpc.RpcServiceImplBase}.
 *
 * <p>Implements only the RPCs needed by FlexLB's batch scheduler:
 * <ul>
 *   <li>{@code enqueueBatch()} — records requests, applies configured delay, returns success/error</li>
 *   <code>getWorkerStatus()} — returns configurable {@link EngineRpcService.WorkerStatusPB}</code>
 *   <li>{@code getCacheStatus()} — returns configurable {@link EngineRpcService.CacheStatusPB}</li>
 *   <li>{@code checkHealth()} — always returns healthy</li>
 * </ul>
 * All other RPCs fall through to the default (UNIMPLEMENTED) behavior.
 *
 * <p>All call records use thread-safe collections ({@link CopyOnWriteArrayList},
 * {@link java.util.concurrent.ConcurrentHashMap}) so they can be read from test threads
 * for assertion without additional synchronization.
 */
public class MockRpcService extends RpcServiceGrpc.RpcServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(MockRpcService.class);

    // ==================== Call records (thread-safe, for assertions) ====================

    /** All EnqueueBatch requests received, in arrival order. */
    final CopyOnWriteArrayList<EngineRpcService.EnqueueBatchRequestPB> enqueuedRequests = new CopyOnWriteArrayList<>();

    /** Counter for GetWorkerStatus calls. */
    final AtomicLong workerStatusCallCount = new AtomicLong(0);

    /** Counter for GetCacheStatus calls. */
    final AtomicLong cacheStatusCallCount = new AtomicLong(0);

    /** Counter for CheckHealth calls. */
    final AtomicLong healthCheckCount = new AtomicLong(0);
    private final AtomicLong cancelCallCount = new AtomicLong();
    private final Object admissionLock = new Object();
    private final Set<Long> admittedRequests = new HashSet<>();
    private final Set<Long> absentFences = new HashSet<>();
    private final Set<Long> fencedEnqueueRejections = new HashSet<>();
    private volatile CountDownLatch enqueueAdmissionGate;
    private volatile CountDownLatch cancelFenceGate;

    // ==================== Behavior (volatile, hot-swappable) ====================

    private volatile MockWorkerBehavior behavior = MockWorkerBehavior.builder().build();

    /**
     * Update behavior at runtime.  Thread-safe — the next RPC will see the new config.
     */
    public void setBehavior(MockWorkerBehavior behavior) {
        this.behavior = behavior;
    }

    public MockWorkerBehavior getBehavior() {
        return behavior;
    }

    // ==================== Accessors for test assertions ====================

    public List<EngineRpcService.EnqueueBatchRequestPB> getEnqueuedRequests() {
        return List.copyOf(enqueuedRequests);
    }

    public int getEnqueueCount() {
        return enqueuedRequests.size();
    }

    public long getWorkerStatusCallCount() {
        return workerStatusCallCount.get();
    }

    public long getCacheStatusCallCount() {
        return cacheStatusCallCount.get();
    }

    public long getHealthCheckCount() {
        return healthCheckCount.get();
    }

    public long getCancelCallCount() {
        return cancelCallCount.get();
    }

    public boolean isAdmitted(long requestId) {
        synchronized (admissionLock) {
            return admittedRequests.contains(requestId);
        }
    }

    public boolean isAbsentFenced(long requestId) {
        synchronized (admissionLock) {
            return absentFences.contains(requestId);
        }
    }

    public boolean wasFencedEnqueueRejected(long requestId) {
        synchronized (admissionLock) {
            return fencedEnqueueRejections.contains(requestId);
        }
    }

    public void setEnqueueAdmissionGate(CountDownLatch gate) {
        enqueueAdmissionGate = gate;
    }

    public void setCancelFenceGate(CountDownLatch gate) {
        cancelFenceGate = gate;
    }

    private static boolean awaitGate(CountDownLatch gate, StreamObserver<?> observer) {
        if (gate == null) {
            return true;
        }
        try {
            if (gate.await(5, TimeUnit.SECONDS)) {
                return true;
            }
            observer.onError(Status.DEADLINE_EXCEEDED
                    .withDescription("test admission gate was not released").asRuntimeException());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            observer.onError(Status.CANCELLED.withCause(e).asRuntimeException());
        }
        return false;
    }

    /** Clear all call records (useful between test cases sharing a worker). */
    public void resetRecords() {
        enqueuedRequests.clear();
        workerStatusCallCount.set(0);
        cacheStatusCallCount.set(0);
        healthCheckCount.set(0);
    }

    // ==================== RPC implementations ====================

    @Override
    public void enqueueBatch(EngineRpcService.EnqueueBatchRequestPB request,
                            StreamObserver<EngineRpcService.EnqueueBatchResponsePB> responseObserver) {
        enqueuedRequests.add(request);
        MockWorkerBehavior beh = behavior;
        log.debug("MockRpcService enqueueBatch: batch_id={}, dp_slots={}, delay_ms={}, fail={}",
                request.getBatchId(), request.getDpSlotsCount(), beh.getEnqueueDelayMs(), beh.isFailOnEnqueue());

        if (!awaitGate(enqueueAdmissionGate, responseObserver)) {
            return;
        }

        if (beh.getEnqueueDelayMs() > 0) {
            try {
                Thread.sleep(beh.getEnqueueDelayMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        EngineRpcService.EnqueueBatchResponsePB.Builder responseBuilder =
                EngineRpcService.EnqueueBatchResponsePB.newBuilder()
                        .setBatchId(request.getBatchId());

        synchronized (admissionLock) {
            for (EngineRpcService.EnqueueBatchDpSlotPB slot : request.getDpSlotsList()) {
                for (EngineRpcService.EnqueueBatchExternalInputPB ext : slot.getRequestsList()) {
                    long reqId = ext.getInput().getRequestId();
                    if (absentFences.contains(reqId) || beh.isFailOnEnqueue()) {
                        boolean fenced = absentFences.contains(reqId);
                        if (fenced) {
                            fencedEnqueueRejections.add(reqId);
                        }
                        responseBuilder.addErrors(EngineRpcService.EnqueueBatchErrorPB.newBuilder()
                                .setRequestId(reqId)
                                .setErrorInfo(EngineRpcService.ErrorDetailsPB.newBuilder()
                                        .setErrorCode(fenced ? 8429L : beh.getEnqueueErrorCode())
                                        .setErrorMessage(fenced ? "request fenced before enqueue"
                                                : beh.getEnqueueErrorMessage())
                                        .build())
                                .build());
                    } else {
                        admittedRequests.add(reqId);
                        responseBuilder.addSuccesses(EngineRpcService.EnqueueBatchSuccessPB.newBuilder()
                                .setRequestId(reqId)
                                .build());
                    }
                }
            }
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void cancel(EngineRpcService.CancelRequestPB request,
                       StreamObserver<EngineRpcService.CancelResponsePB> observer) {
        cancelCallCount.incrementAndGet();
        if (behavior.getRoleType() != EngineRpcService.RoleTypePB.ROLE_TYPE_PREFILL) {
            observer.onError(Status.UNIMPLEMENTED.withDescription("Cancel belongs to Prefill")
                    .asRuntimeException());
            return;
        }
        if (!awaitGate(cancelFenceGate, observer)) {
            return;
        }
        EngineRpcService.CancelStatusPB result;
        synchronized (admissionLock) {
            if (admittedRequests.contains(request.getRequestId())) {
                // This small RPC fixture has no execution/typed-terminal model.
                // A previously admitted request must never be called absent.
                result = EngineRpcService.CancelStatusPB.CANCEL_STATUS_NOT_FOUND;
            } else {
                absentFences.add(request.getRequestId());
                result = EngineRpcService.CancelStatusPB.CANCEL_STATUS_TOMBSTONED;
            }
        }
        observer.onNext(EngineRpcService.CancelResponsePB.newBuilder().setStatus(result).build());
        observer.onCompleted();
    }

    @Override
    public void getWorkerStatus(EngineRpcService.StatusVersionPB request,
                                StreamObserver<EngineRpcService.WorkerStatusPB> responseObserver) {
        workerStatusCallCount.incrementAndGet();
        MockWorkerBehavior beh = behavior;

        EngineRpcService.WorkerStatusPB.Builder builder = EngineRpcService.WorkerStatusPB.newBuilder()
                .setAlive(true)
                .setRole(RoleTypeProtoConverter.toLegacyWorkerStatusString(
                        RoleTypeProtoConverter.fromProto(beh.getRoleType())))
                .setRoleType(beh.getRoleType())
                .setAvailableConcurrency(beh.getAvailableConcurrency())
                .setAvailableKvCache(beh.getAvailableKvCache())
                .setTotalKvCache(beh.getTotalKvCache())
                .setStatusVersion(request.getLatestCacheVersion() + 1)
                .setLatestFinishedVersion(request.getLatestFinishedVersion())
                .setDpSize(1)
                .setTpSize(1)
                .setDpRank(0);

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getCacheStatus(EngineRpcService.CacheVersionPB request,
                               StreamObserver<EngineRpcService.CacheStatusPB> responseObserver) {
        cacheStatusCallCount.incrementAndGet();
        MockWorkerBehavior beh = behavior;

        EngineRpcService.CacheStatusPB.Builder builder = EngineRpcService.CacheStatusPB.newBuilder()
                .setAvailableKvCache(beh.getAvailableKvCache())
                .setTotalKvCache(beh.getTotalKvCache())
                .setBlockSize(1024)
                .setVersion(request.getLatestCacheVersion() + 1);

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void checkHealth(EngineRpcService.EmptyPB request,
                            StreamObserver<EngineRpcService.CheckHealthResponsePB> responseObserver) {
        healthCheckCount.incrementAndGet();
        responseObserver.onNext(EngineRpcService.CheckHealthResponsePB.newBuilder()
                .setHealth("ok")
                .build());
        responseObserver.onCompleted();
    }
}
