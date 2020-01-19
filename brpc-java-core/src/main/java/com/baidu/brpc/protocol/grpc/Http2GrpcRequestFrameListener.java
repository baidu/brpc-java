package com.baidu.brpc.protocol.grpc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.*;
import lombok.Getter;

public class Http2GrpcRequestFrameListener extends Http2EventAdapter {

    @Getter
    private Http2GrpcRequest http2GrpcRequest;

    public void onHeadersRead(ChannelHandlerContext ctx, final int streamId, Http2Headers headers, int streamDependency, short weight, boolean exclusive, int padding, boolean endStream) throws Http2Exception {
        Http2HeadersFrame frame=new DefaultHttp2HeadersFrame(headers,endStream,padding);
        if (http2GrpcRequest == null){
            http2GrpcRequest = new Http2GrpcRequest();

        }
        http2GrpcRequest.setHttp2Headers(frame);
        http2GrpcRequest.setStreamId(streamId);
    }
    public int onDataRead(ChannelHandlerContext ctx,final int streamId, ByteBuf data, int padding, boolean endOfStream) throws Http2Exception {
        Http2DataFrame frame=new DefaultHttp2DataFrame(data,endOfStream,padding);
        if (http2GrpcRequest == null){
            http2GrpcRequest = new Http2GrpcRequest();

        }
        http2GrpcRequest.setHttp2Data(frame);
        http2GrpcRequest.setStreamId(streamId);

        return data.readableBytes() + padding;
    }

}