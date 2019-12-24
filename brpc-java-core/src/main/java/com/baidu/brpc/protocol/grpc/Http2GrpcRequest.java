package com.baidu.brpc.protocol.grpc;

import com.baidu.brpc.protocol.AbstractRequest;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import lombok.Getter;
import lombok.Setter;

public class Http2GrpcRequest extends AbstractRequest {


    @Getter
    @Setter
    private Http2HeadersFrame http2Headers;
    @Getter
    @Setter
    private Http2DataFrame http2Data;
    @Getter
    @Setter
    private boolean isEndOfStream;

}
