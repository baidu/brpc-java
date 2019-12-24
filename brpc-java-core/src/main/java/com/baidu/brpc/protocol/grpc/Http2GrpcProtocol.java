package com.baidu.brpc.protocol.grpc;

import com.baidu.brpc.RpcMethodInfo;
import com.baidu.brpc.buffer.DynamicCompositeByteBuf;
import com.baidu.brpc.compress.Compress;
import com.baidu.brpc.compress.CompressManager;
import com.baidu.brpc.exceptions.BadSchemaException;
import com.baidu.brpc.exceptions.NotEnoughDataException;
import com.baidu.brpc.exceptions.TooBigDataException;
import com.baidu.brpc.protocol.AbstractProtocol;
import com.baidu.brpc.protocol.Options;
import com.baidu.brpc.protocol.Request;
import com.baidu.brpc.protocol.Response;
import com.baidu.brpc.server.ServiceManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Http2GrpcProtocol extends AbstractProtocol {

    private static final CompressManager compressManager = CompressManager.getInstance();
    private static final ServiceManager serviceManager = ServiceManager.getInstance();

    private Map<String, Http2ConnectionHandler> handlerMap = new ConcurrentHashMap<String, Http2ConnectionHandler>();


    @Override
    public Object decode(final ChannelHandlerContext ctx, DynamicCompositeByteBuf in, boolean isDecodingRequest) throws BadSchemaException, TooBigDataException, NotEnoughDataException {


        String channelId = ctx.channel().id().asLongText();

        Http2ConnectionHandler handler = handlerMap.get(channelId);

        try {
            if (handler == null) {

                Http2Connection connection = new DefaultHttp2Connection(true);
                Http2ConnectionEncoder encoder = new DefaultHttp2ConnectionEncoder(connection, new DefaultHttp2FrameWriter());
                Http2ConnectionDecoder decoder = new DefaultHttp2ConnectionDecoder(connection, encoder, new DefaultHttp2FrameReader(), Http2PromisedRequestVerifier.ALWAYS_VERIFY, true);

                handler = new Http2ConnectionHandler(true, new DefaultHttp2FrameWriter(),
                        null, new Http2Settings());

                handler.handlerAdded(ctx);

                decoder.lifecycleManager(handler);

                handlerMap.put(channelId, handler);

            }

            ByteBuf readyToDecode = in.readRetainedSlice(in.readableBytes());

            FrameListener frameListener = new FrameListener();
            handler.decoder().frameListener(frameListener);
            handler.decode(ctx, readyToDecode, new ArrayList());

            Http2GrpcRequest request = frameListener.getHttp2GrpcRequest();

            if (request != null) {

                Http2Headers requestHeaders = request.getHttp2Headers().headers();
                CharSequence path = requestHeaders.path();
                String pathStr = path.toString();
                String[] arr = pathStr.split("/");
                String serviceName = arr[1];
                String methodName = arr[2];

                int compressType = Options.CompressType.COMPRESS_TYPE_NONE_VALUE;
                request.setCompressType(compressType);

                RpcMethodInfo rpcMethodInfo = serviceManager.getService(
                        serviceName, methodName);


                ByteBuf protoAndAttachmentBuf = request.getHttp2Data().content();

                byte compressFlag = protoAndAttachmentBuf.readByte();
                int messageLength = protoAndAttachmentBuf.readInt();

                Compress compress = compressManager.getCompress(compressType);
                Object proto = compress.uncompressInput(protoAndAttachmentBuf, rpcMethodInfo);

                request.setArgs(new Object[]{proto});
                request.setRpcMethodInfo(rpcMethodInfo);
                request.setTargetMethod(rpcMethodInfo.getMethod());
                request.setTarget(rpcMethodInfo.getTarget());
                Map<String, Object> attachment = new HashMap<String, Object>();
                attachment.put("streamId", Integer.toString(request.getHttp2Headers().stream().id()));
                attachment.put("ctx", ctx);
                request.setKvAttachment(attachment);
                return request;
            }

        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (handler != null) {
                    handler.close(ctx, ctx.newPromise());
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                handlerMap.remove(channelId);
            }
        }

        return null;
    }

    @Override
    public ByteBuf encodeRequest(Request request) throws Exception {
        //return null;
        throw new UnsupportedOperationException("encodeRequest is not support in grpc protocol");
    }

    @Override
    public Response decodeResponse(Object msg, ChannelHandlerContext ctx) throws Exception {
        throw new UnsupportedOperationException("decodeResponse is not support in grpc protocol");

    }

    @Override
    public Request decodeRequest(Object packet) throws Exception {
        return (Request) packet;
    }

    @Override
    public ByteBuf encodeResponse(Request request, Response response) throws Exception {

        if (request == null) {
            return Unpooled.EMPTY_BUFFER;
        } else {
            Object responseProto = response.getResult();
            Channel channel = request.getChannel();
            String channelId = channel.id().asLongText();

            Http2ConnectionHandler handler = handlerMap.get(channelId);

            int streamId = Integer.parseInt(request.getKvAttachment().get("streamId").toString());
            ChannelHandlerContext ctx = (ChannelHandlerContext) request.getKvAttachment().get("ctx");

            Http2Headers responseHeader = new DefaultHttp2Headers();
            responseHeader.status("200");
            responseHeader.add("content-type", "application/grpc+proto");

            Http2Headers responseEndHeader = new DefaultHttp2Headers();
            responseEndHeader.add("grpc-status", "0");

            if (responseProto != null) {
                Compress compress = compressManager.getCompress(request.getCompressType());
                ByteBuf protoBuf = compress.compressOutput(responseProto, response.getRpcMethodInfo());
                ByteBuf resultDataBuf = ctx.alloc().buffer(2 + 4 + protoBuf.readableBytes());

                resultDataBuf.writeShort(0); // compress flag (0 means no compress)
                resultDataBuf.writeMedium(protoBuf.readableBytes()); // data length
                resultDataBuf.writeBytes(protoBuf); // data content


                ChannelPromise promise = ctx.newPromise();
                Http2FrameWriter frameWriter = handler.encoder().frameWriter();
                frameWriter.writeHeaders(ctx,streamId,responseHeader,0,false,promise);
                frameWriter.writeData(ctx, streamId, resultDataBuf, 0, false, promise);
                frameWriter.writeHeaders(ctx,streamId,responseEndHeader,0,true,promise);

            }

            return Unpooled.EMPTY_BUFFER;
        }
    }
}
