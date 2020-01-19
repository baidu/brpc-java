package com.baidu.brpc.protocol.grpc;

import com.baidu.brpc.protocol.Options;
import com.baidu.brpc.protocol.Protocol;
import com.baidu.brpc.protocol.ProtocolFactory;

public class Http2GrpcProtocolFactory implements ProtocolFactory {

    @Override
    public Integer getProtocolType() {
        return Options.ProtocolType.PROTOCOL_GRPC_PROTOBUF_VALUE;
    }

    public Integer getPriority() {
        return ProtocolFactory.DEFAULT_PRIORITY - 1;
    }

    @Override
    public Protocol createProtocol(String encoding) {
        return new Http2GrpcProtocol();
    }
}