/*
 * Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.baidu.brpc.protocol.standard;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.brpc.ChannelInfo;
import com.baidu.brpc.RpcMethodInfo;
import com.baidu.brpc.buffer.DynamicCompositeByteBuf;
import com.baidu.brpc.client.RpcFuture;
import com.baidu.brpc.compress.Compress;
import com.baidu.brpc.compress.CompressManager;
import com.baidu.brpc.exceptions.BadSchemaException;
import com.baidu.brpc.exceptions.NotEnoughDataException;
import com.baidu.brpc.exceptions.RpcException;
import com.baidu.brpc.exceptions.TooBigDataException;
import com.baidu.brpc.protocol.AbstractProtocol;
import com.baidu.brpc.protocol.BaiduRpcErrno;
import com.baidu.brpc.protocol.Request;
import com.baidu.brpc.protocol.Response;
import com.baidu.brpc.protocol.RpcResponse;
import com.baidu.brpc.server.ServiceManager;
import com.baidu.brpc.utils.ProtobufUtils;
import com.google.protobuf.ByteString;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

/**
 * Notes on Baidu RPC Protocol:
 * <ul>
 * <li> Header format is ["PRPC"][<code>body_size</code>][<code>meta_size
 * </code>], 12 bytes in total
 * </li>
 * <li> {@code body_size} and {@code meta_size} are in <b>network</b> byte
 * order (big endian)
 * </li>
 * <li> Use service->full_name() and method_name to identify the service and
 * method to call </li>
 * <li> {@code attachment_size} is set iff request/response has attachment </li>
 * <li> The following fields of rpc are not supported yet:
 * <ul>
 * <li>chunk_info</li>
 * </ul>
 * </li>
 * </ul>
 * Created by huwenwei on 2017/9/22.
 */
public class BaiduRpcProtocol extends AbstractProtocol {

    private static final Logger LOG = LoggerFactory.getLogger(BaiduRpcProtocol.class);
    private static final byte[] MAGIC_HEAD = "PRPC".getBytes();
    private static final int FIXED_LEN = 12;
    private static final BaiduRpcProto.RpcMeta defaultRpcMetaInstance = BaiduRpcProto.RpcMeta.getDefaultInstance();
    private static final CompressManager compressManager = CompressManager.getInstance();
    private static final ServiceManager serviceManager = ServiceManager.getInstance();

    @Override
    public ByteBuf encodeRequest(Request request) throws Exception {
        BaiduRpcEncodePacket packet = new BaiduRpcEncodePacket();
        BaiduRpcProto.RpcMeta.Builder metaBuilder = BaiduRpcProto.RpcMeta.newBuilder();
        metaBuilder.setCorrelationId(request.getCorrelationId());
        int compressType = request.getCompressType();
        metaBuilder.setCompressType(request.getCompressType());

        BaiduRpcProto.RpcRequestMeta.Builder requestMeta = BaiduRpcProto.RpcRequestMeta.newBuilder();
        requestMeta.setLogId(request.getLogId());
        requestMeta.setServiceName(request.getServiceName());
        requestMeta.setMethodName(request.getMethodName());
        if (request.getTraceId() != null) {
            requestMeta.setTraceId(request.getTraceId());
        }
        if (request.getSpanId() != null) {
            requestMeta.setSpanId(request.getSpanId());
        }
        if (request.getParentSpanId() != null) {
            requestMeta.setParentSpanId(request.getParentSpanId());
        }
        if (request.getKvAttachment() != null) {
            for (Map.Entry<String, Object> kv : request.getKvAttachment().entrySet()) {
                requestMeta.addExtFieldsBuilder().setKey(kv.getKey()).setValue((String) kv.getValue());
            }
        }
        metaBuilder.setRequest(requestMeta.build());

        // proto
        Compress compress = compressManager.getCompress(compressType);
        ByteBuf protoBuf = compress.compressInput(request.getArgs()[0], request.getRpcMethodInfo());
        packet.setProto(protoBuf);

        // attachment
        if (request.getBinaryAttachment() != null
                && request.getBinaryAttachment().isReadable()) {
            packet.setAttachment(request.getBinaryAttachment());
            metaBuilder.setAttachmentSize(request.getBinaryAttachment().readableBytes());
        }
        // auth
        if (request.getAuth() != null) {
            metaBuilder.setAuthenticationData(ByteString.copyFrom(request.getAuth().getBytes()));
        }
        packet.setRpcMeta(metaBuilder.build());

        return encode(packet);
    }

    public RpcResponse decodeResponse(Object packet, ChannelHandlerContext ctx) throws Exception {
        BaiduRpcDecodePacket responsePacket = (BaiduRpcDecodePacket) packet;
        ByteBuf metaBuf = responsePacket.getMetaBuf();
        ByteBuf protoAndAttachmentBuf = responsePacket.getProtoAndAttachmentBuf();
        try {
            BaiduRpcProto.RpcMeta rpcMeta = (BaiduRpcProto.RpcMeta) ProtobufUtils.parseFrom(
                    metaBuf, defaultRpcMetaInstance);
            RpcResponse rpcResponse = new RpcResponse();
            long correlationId = rpcMeta.getCorrelationId();
            rpcResponse.setCorrelationId(correlationId);

            ChannelInfo channelInfo = ChannelInfo.getClientChannelInfo(ctx.channel());
            RpcFuture future = channelInfo.removeRpcFuture(rpcResponse.getCorrelationId());
            if (future == null) {
                return rpcResponse;
            }
            rpcResponse.setRpcFuture(future);
            BaiduRpcProto.RpcResponseMeta responseMeta = rpcMeta.getResponse();
            try {
                if (responseMeta != null && responseMeta.getErrorCode() == 0) {
                    Compress compress = compressManager.getCompress(rpcMeta.getCompressType());
                    int protoSize = protoAndAttachmentBuf.readableBytes()
                            - rpcMeta.getAttachmentSize();
                    // proto body
                    ByteBuf protoBuf = protoAndAttachmentBuf.readSlice(protoSize);
                    Object proto = compress.uncompressOutput(protoBuf, future.getRpcMethodInfo());
                    rpcResponse.setResult(proto);

                    // attachment
                    if (rpcMeta.getAttachmentSize() > 0) {
                        rpcResponse.setBinaryAttachment(protoAndAttachmentBuf);
                        protoAndAttachmentBuf = null;
                    }
                } else {
                    rpcResponse.setException(new RpcException(
                            RpcException.SERVICE_EXCEPTION, responseMeta.getErrorText()));
                }
            } catch (Exception ex) {
                // 解析失败直接抛异常
                throw new RpcException(RpcException.SERIALIZATION_EXCEPTION, "decode response failed", ex);
            }
            return rpcResponse;
        } finally {
            if (metaBuf != null) {
                metaBuf.release();
            }
            if (protoAndAttachmentBuf != null) {
                protoAndAttachmentBuf.release();
            }
        }
    }

    @Override
    public BaiduRpcDecodePacket decode(ChannelHandlerContext ctx, DynamicCompositeByteBuf in, boolean isDecodingRequest)
            throws BadSchemaException, TooBigDataException, NotEnoughDataException {
        if (in.readableBytes() < FIXED_LEN) {
            throw notEnoughDataException;
        }
        ByteBuf fixHeaderBuf = in.retainedSlice(FIXED_LEN);
        try {
            byte[] magic = new byte[4];
            fixHeaderBuf.readBytes(magic);
            if (!Arrays.equals(magic, MAGIC_HEAD)) {
                throw new BadSchemaException("not valid magic head for brpc");
            }

            int bodySize = fixHeaderBuf.readInt();

            // 512M
            if (bodySize > 512 * 1024 * 1024) {
                throw new TooBigDataException("to big body size:" + bodySize);
            }

            if (in.readableBytes() < FIXED_LEN + bodySize) {
                throw notEnoughDataException;
            }

            int metaSize = fixHeaderBuf.readInt();
            in.skipBytes(FIXED_LEN);
            BaiduRpcDecodePacket packet = new BaiduRpcDecodePacket();
            try {
                // meta
                ByteBuf metaBuf = in.readRetainedSlice(metaSize);
                packet.setMetaBuf(metaBuf);

                // proto and attachment
                ByteBuf protoAndAttachmentBuf = in.readRetainedSlice(bodySize - metaSize);
                packet.setProtoAndAttachmentBuf(protoAndAttachmentBuf);

                return packet;
            } catch (Exception ex) {
                LOG.warn("decode failed:", ex);
                throw new RpcException(RpcException.SERIALIZATION_EXCEPTION, ex);
            }
        } finally {
            fixHeaderBuf.release();
        }
    }

    @Override
    public Request decodeRequest(Object packet) throws Exception {
        Request request = this.createRequest();
        BaiduRpcDecodePacket requestPacket = (BaiduRpcDecodePacket) packet;
        ByteBuf metaBuf = requestPacket.getMetaBuf();
        ByteBuf protoAndAttachmentBuf = requestPacket.getProtoAndAttachmentBuf();
        BaiduRpcProto.RpcMeta rpcMeta;
        try {
            rpcMeta = (BaiduRpcProto.RpcMeta) ProtobufUtils.parseFrom(
                    metaBuf, defaultRpcMetaInstance);
            BaiduRpcProto.RpcRequestMeta requestMeta = rpcMeta.getRequest();
            request.setCorrelationId(rpcMeta.getCorrelationId());
            int compressType = rpcMeta.getCompressType();
            request.setCompressType(compressType);
            request.setLogId(requestMeta.getLogId());

            RpcMethodInfo rpcMethodInfo = serviceManager.getService(
                    requestMeta.getServiceName(), requestMeta.getMethodName());
            if (rpcMethodInfo == null) {
                String errorMsg = String.format("Fail to find service=%s, method=%s",
                        requestMeta.getServiceName(), requestMeta.getMethodName());
                LOG.error(errorMsg);
                request.setException(new RpcException(RpcException.SERVICE_EXCEPTION, errorMsg));
                return request;
            }
            if (requestMeta.hasTraceId()) {
                request.setTraceId(requestMeta.getTraceId());
            }
            if (requestMeta.hasSpanId()) {
                request.setSpanId(requestMeta.getSpanId());
            }
            if (requestMeta.hasParentSpanId()) {
                request.setParentSpanId(requestMeta.getParentSpanId());
            }
            if (requestMeta.getExtFieldsCount() > 0) {
                if (request.getKvAttachment() == null) {
                    request.setKvAttachment(new HashMap<String, Object>());
                }
                for (BaiduRpcProto.RpcRequestMetaExtField extField : requestMeta.getExtFieldsList()) {
                    request.getKvAttachment().put(extField.getKey(), extField.getValue());
                }
            }
            request.setServiceName(rpcMethodInfo.getServiceName());
            request.setMethodName(rpcMethodInfo.getMethodName());
            request.setRpcMethodInfo(rpcMethodInfo);
            request.setTargetMethod(rpcMethodInfo.getMethod());
            request.setTarget(rpcMethodInfo.getTarget());

            // auth
            if (rpcMeta.getAuthenticationData() != null) {
                request.setAuth(new String(rpcMeta.getAuthenticationData().toByteArray()));
            }

            // proto body
            Compress compress = compressManager.getCompress(compressType);
            if (rpcMeta.hasAttachmentSize() && rpcMeta.getAttachmentSize() > 0) {
                int protoSize = protoAndAttachmentBuf.readableBytes() - rpcMeta.getAttachmentSize();
                ByteBuf protoBuf = protoAndAttachmentBuf.readSlice(protoSize);
                Object proto = compress.uncompressInput(protoBuf, rpcMethodInfo);
                request.setArgs(new Object[] {proto});
                request.setBinaryAttachment(protoAndAttachmentBuf);
                protoAndAttachmentBuf = null;
            } else {
                Object proto = compress.uncompressInput(protoAndAttachmentBuf, rpcMethodInfo);
                request.setArgs(new Object[] {proto});
            }
            return request;
        } finally {
            if (metaBuf != null) {
                metaBuf.release();
            }
            if (protoAndAttachmentBuf != null) {
                protoAndAttachmentBuf.release();
            }
        }
    }

    @Override
    public ByteBuf encodeResponse(Request request, Response response) throws Exception {
        BaiduRpcEncodePacket responsePacket = new BaiduRpcEncodePacket();
        BaiduRpcProto.RpcMeta.Builder metaBuilder = BaiduRpcProto.RpcMeta.newBuilder();
        metaBuilder.setCorrelationId(response.getCorrelationId());
        int compressType = response.getCompressType();
        metaBuilder.setCompressType(compressType);
        BaiduRpcProto.RpcResponseMeta.Builder responseMetaBuilder = BaiduRpcProto.RpcResponseMeta.newBuilder();

        if (response.getException() != null) {
            responseMetaBuilder.setErrorCode(BaiduRpcErrno.Errno.EINTERNAL_VALUE);
            if (StringUtils.isNotBlank(response.getException().getMessage())) {
                responseMetaBuilder.setErrorText(response.getException().getMessage());
            }
            metaBuilder.setResponse(responseMetaBuilder.build());
            responsePacket.setRpcMeta(metaBuilder.build());
        } else {
            responseMetaBuilder.setErrorCode(0);
            metaBuilder.setResponse(responseMetaBuilder.build());

            Object responseProto = response.getResult();
            Compress compress = compressManager.getCompress(compressType);
            ByteBuf protoBuf = compress.compressOutput(responseProto, response.getRpcMethodInfo());
            responsePacket.setProto(protoBuf);

            // attachment
            if (response.getBinaryAttachment() != null) {
                responsePacket.setAttachment(response.getBinaryAttachment());
                metaBuilder.setAttachmentSize(response.getBinaryAttachment().readableBytes());
            }
            responsePacket.setRpcMeta(metaBuilder.build());
        }

        return encode(responsePacket);
    }

    protected ByteBuf encode(BaiduRpcEncodePacket packet) throws Exception {
        byte[] metaBytes = packet.getRpcMeta().toByteArray();
        ByteBuf metaBuf = Unpooled.wrappedBuffer(metaBytes);
        int metaSize = metaBytes.length;

        // fixed header buf
        ByteBuf headerBuf = Unpooled.buffer(FIXED_LEN);
        headerBuf.writeBytes(MAGIC_HEAD);
        int bodySize = metaSize;
        ByteBuf protoBuf = packet.getProto();
        if (protoBuf != null) {
            bodySize += protoBuf.readableBytes();
        }
        ByteBuf attachmentBuf = packet.getAttachment();
        if (attachmentBuf != null) {
            bodySize += attachmentBuf.readableBytes();
        }
        headerBuf.writeInt(bodySize);
        headerBuf.writeInt(metaSize);
        if (protoBuf != null && attachmentBuf != null) {
            return Unpooled.wrappedBuffer(headerBuf, metaBuf, protoBuf, attachmentBuf);
        } else if (protoBuf != null) {
            return Unpooled.wrappedBuffer(headerBuf, metaBuf, protoBuf);
        } else {
            return Unpooled.wrappedBuffer(headerBuf, metaBuf);
        }
    }

    @Override
    public boolean isCoexistence() {
        return true;
    }

}
