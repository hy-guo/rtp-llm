package org.flexlb.engine.grpc;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.CodedOutputStream;

import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCalls;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;

class CacheStatusMarshallerTest {
    @Test
    void matchesGeneratedParserAcrossSignedKeysDuplicatesFalseValuesAndUnknownFields()
            throws Exception {
        var random = new Random(1727);
        for (int round = 0; round < 100; round++) {
            var message =
                    EngineRpcService.CacheStatusPB.newBuilder()
                            .setVersion(round)
                            .setAvailableKvCache(17)
                            .setTotalKvCache(91)
                            .setBlockSize(512);
            for (long key : new long[] {0, -1, Long.MIN_VALUE, Long.MAX_VALUE})
                message.putCacheKeys(key, false);
            for (int i = 0; i < 1000; i++)
                message.putCacheKeys(random.nextLong(), random.nextBoolean());
            var bytes = new ByteArrayOutputStream();
            bytes.write(message.build().toByteArray());
            var output = CodedOutputStream.newInstance(bytes);
            output.writeByteArray(99, new byte[] {1, 2, 3});
            output.writeByteArray(5, new byte[] {8, 0, 16, 1});
            output.writeByteArray(5, new byte[0]);
            output.flush();
            var expected = EngineRpcService.CacheStatusPB.parseFrom(bytes.toByteArray());
            var actual =
                    CacheStatusMarshaller.INSTANCE.parse(
                            new ByteArrayInputStream(bytes.toByteArray()));
            assertEquals(expected.getVersion(), actual.status().getVersion());
            assertEquals(expected.getAvailableKvCache(), actual.status().getAvailableKvCache());
            assertEquals(expected.getTotalKvCache(), actual.status().getTotalKvCache());
            assertEquals(expected.getBlockSize(), actual.status().getBlockSize());
            assertEquals(expected.getCacheKeysMap().keySet(), actual.status().getCachedKeys());
            assertEquals(expected.getCacheKeysCount(), actual.status().getCacheKeySize());
            assertEquals(bytes.size(), actual.serializedSize());
        }
    }

    @Test
    void malformedResponsesFailWithGrpcProtobufStatus() {
        for (byte[] bytes :
                new byte[][] {{42, 5, 8, 1}, {42, -1, -1, -1, -1, 15}, {12}, {42, 1, 12}}) {
            var error =
                    assertThrows(
                            StatusRuntimeException.class,
                            () ->
                                    CacheStatusMarshaller.INSTANCE.parse(
                                            new ByteArrayInputStream(bytes)));
            assertEquals(Status.Code.INTERNAL, error.getStatus().getCode());
        }
    }

    @Test
    void tcpRpcAcceptsLargeCacheResponseOnlyWithPerCallLimit() throws Exception {
        var bytes = new ByteArrayOutputStream();
        var output = CodedOutputStream.newInstance(bytes);
        output.writeByteArray(99, new byte[17 << 20]);
        output.writeByteArray(5, new byte[] {8, 123, 16, 1});
        output.flush();
        byte[] wire = bytes.toByteArray();
        var raw =
                new MethodDescriptor.Marshaller<byte[]>() {
                    public InputStream stream(byte[] value) {
                        return new ByteArrayInputStream(value);
                    }

                    public byte[] parse(InputStream input) {
                        throw new UnsupportedOperationException();
                    }
                };
        var original = RpcServiceGrpc.getGetCacheStatusMethod();
        var method = original.toBuilder(original.getRequestMarshaller(), raw).build();
        var definition =
                ServerServiceDefinition.builder(RpcServiceGrpc.SERVICE_NAME)
                        .addMethod(
                                method,
                                ServerCalls.asyncUnaryCall(
                                        (request, observer) -> {
                                            observer.onNext(wire);
                                            observer.onCompleted();
                                        }))
                        .build();
        var server = NettyServerBuilder.forPort(0).addService(definition).build().start();
        var channel =
                ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                        .usePlaintext()
                        .maxInboundMessageSize(16 << 20)
                        .build();
        try {
            var stub =
                    RpcServiceGrpc.newFutureStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
            var request = EngineRpcService.CacheVersionPB.getDefaultInstance();
            var failure =
                    assertThrows(
                            java.util.concurrent.ExecutionException.class,
                            () ->
                                    CacheStatusMarshaller.call(stub, request, 16 << 20)
                                            .get(10, TimeUnit.SECONDS));
            assertEquals(
                    Status.Code.RESOURCE_EXHAUSTED,
                    Status.fromThrowable(failure.getCause()).getCode());
            var response =
                    CacheStatusMarshaller.call(stub, request, 64 << 20).get(10, TimeUnit.SECONDS);
            assertEquals(java.util.Set.of(123L), response.status().getCachedKeys());
            assertEquals(wire.length, response.serializedSize());
            assertNull(stub.getCallOptions().getMaxInboundMessageSize());
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
