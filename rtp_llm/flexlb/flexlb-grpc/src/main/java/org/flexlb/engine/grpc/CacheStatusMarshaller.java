package org.flexlb.engine.grpc;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.ClientCalls;

import org.flexlb.collection.LongKeySet;
import org.flexlb.dao.master.CacheStatus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/** CacheStatusPB wire format decoded into an owned primitive key snapshot. */
public final class CacheStatusMarshaller
        implements MethodDescriptor.Marshaller<CacheStatusMarshaller.Response> {
    public record Response(CacheStatus status, int serializedSize) {}

    static final CacheStatusMarshaller INSTANCE = new CacheStatusMarshaller();
    static final MethodDescriptor<EngineRpcService.CacheVersionPB, Response> METHOD =
            RpcServiceGrpc.getGetCacheStatusMethod().toBuilder(
                            RpcServiceGrpc.getGetCacheStatusMethod().getRequestMarshaller(),
                            INSTANCE)
                    .build();

    static ListenableFuture<Response> call(
            RpcServiceGrpc.RpcServiceFutureStub stub,
            EngineRpcService.CacheVersionPB request,
            int limit) {
        return ClientCalls.futureUnaryCall(
                stub.getChannel()
                        .newCall(METHOD, stub.getCallOptions().withMaxInboundMessageSize(limit)),
                request);
    }

    @Override
    public InputStream stream(Response response) {
        var status = response.status();
        var message =
                EngineRpcService.CacheStatusPB.newBuilder()
                        .setAvailableKvCache(status.getAvailableKvCache())
                        .setTotalKvCache(status.getTotalKvCache())
                        .setBlockSize(status.getBlockSize())
                        .setVersion(status.getVersion());
        if (status.getCachedKeys() != null)
            for (long key : status.getCachedKeys()) message.putCacheKeys(key, true);
        return new ByteArrayInputStream(message.build().toByteArray());
    }

    @Override
    public Response parse(InputStream stream) {
        try {
            var input = CodedInputStream.newInstance(stream);
            input.setSizeLimit(Integer.MAX_VALUE);
            var status = CacheStatus.builder().version(0).build();
            var keys = new LongKeySet();
            int tag;
            while ((tag = input.readTag()) != 0) {
                switch (tag) {
                    case 8 -> status.setAvailableKvCache(input.readInt64());
                    case 16 -> status.setTotalKvCache(input.readInt64());
                    case 24 -> status.setBlockSize(input.readInt64());
                    case 32 -> status.setVersion(input.readInt64());
                    case 42 -> {
                        int length = input.readRawVarint32();
                        int limit = input.pushLimit(length);
                        long key = 0;
                        int entryTag;
                        while ((entryTag = input.readTag()) != 0) {
                            if (entryTag == 8) key = input.readInt64();
                            else if (entryTag == 16) input.readBool();
                            else if (!input.skipField(entryTag))
                                throw new InvalidProtocolBufferException(
                                        "unexpected cache entry end group");
                        }
                        input.checkLastTagWas(0);
                        if (input.getBytesUntilLimit() != 0)
                            throw new InvalidProtocolBufferException("truncated cache entry");
                        input.popLimit(limit);
                        keys.addLong(key);
                    }
                    default -> {
                        if (!input.skipField(tag))
                            throw new InvalidProtocolBufferException(
                                    "unexpected cache status end group");
                    }
                }
            }
            input.checkLastTagWas(0);
            status.setCachedKeys(keys);
            status.setCacheKeySize(keys.size());
            return new Response(status, input.getTotalBytesRead());
        } catch (IOException error) {
            throw Status.INTERNAL
                    .withDescription("Invalid cache status protobuf response")
                    .withCause(error)
                    .asRuntimeException();
        }
    }
}
