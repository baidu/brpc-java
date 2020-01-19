package com.baidu.brpc.protocol.grpc;

import com.baidu.brpc.protocol.AbstractResponse;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2Settings;
import lombok.Getter;
import lombok.Setter;

public class Http2GrpcResponse extends AbstractResponse {


    @Getter
    @Setter
    private Http2HeadersFrame startHttp2Headers;
    @Getter
    @Setter
    private Http2HeadersFrame endHttp2Headers;
    @Getter
    @Setter
    private Http2DataFrame http2Data;
    @Getter
    @Setter
    private Http2Settings http2Settings;
    @Getter
    @Setter
    private boolean isEndOfStream;
    @Getter
    @Setter
    private String serviceName;
    @Getter
    @Setter
    private String methodName;
    @Getter
    @Setter
    private int streamId;


}