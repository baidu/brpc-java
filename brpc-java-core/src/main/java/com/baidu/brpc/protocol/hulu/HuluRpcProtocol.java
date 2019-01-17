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

package com.baidu.brpc.protocol.hulu;

import java.io.IOException;
import java.util.Arrays;

import com.baidu.brpc.exceptions.TooBigDataException;
import com.baidu.brpc.protocol.AbstractProtocol;
import com.baidu.brpc.protocol.BaiduRpcErrno;
import com.baidu.brpc.protocol.RpcRequest;
import com.baidu.brpc.protocol.RpcResponse;
import com.baidu.brpc.exceptions.RpcException;
import com.baidu.brpc.RpcMethodInfo;
import com.baidu.brpc.buffer.DynamicCompositeByteBuf;
import com.baidu.brpc.exceptions.BadSchemaException;
import com.baidu.brpc.exceptions.NotEnoughDataException;
import com.baidu.brpc.utils.ProtobufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.brpc.ChannelInfo;
import com.baidu.brpc.client.RpcFuture;
import com.baidu.brpc.compress.Compress;
import com.baidu.brpc.compress.CompressManager;
import com.baidu.brpc.server.ServiceManager;
import com.baidu.brpc.utils.RpcMetaUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

/**
 * Notes on HULU PBRPC Protocol:
 * <ul>
 *     <li> Header format is ["HULU"][<code>body_size</code>][<code>meta_size
 *         </code>], 12 bytes in total
 *     </li>
 *     <li> {@code body_size} and {@code meta_size} are <b>NOT</b> in
 *         <b>network</b> byte order (little endian)
 *     </li>
 *     <li> Use service->name() and method_index to identify the service and
 *         method to call </li>
 *     <li> {@code user_message_size} is set iff request/response has attachment
 *     </li>
 *     <li> The following fields of rpc are not supported yet:
 *         <ul><li>chunk_info</li></ul>
 *     </li>
 * </ul>
 */
public class HuluRpcProtocol extends AbstractProtocol<HuluRpcDecodeBasePacket> {
    private static final Logger LOG = LoggerFactory.getLogger(HuluRpcProtocol.class);
    private static final byte[] MAGIC_HEAD = "HULU".getBytes();
    private static final int FIXED_LEN = 12;
    private static final HuluRpcProto.HuluRpcRequestMeta defaultRpcRequestMetaInstance =
            HuluRpcProto.HuluRpcRequestMeta.getDefaultInstance();
    private static final HuluRpcProto.HuluRpcResponseMeta defaultRpcResponseMetaInstance =
            HuluRpcProto.HuluRpcResponseMeta.getDefaultInstance();
    private static final CompressManager compressManager = CompressManager.getInstance();

    @Override
    public ByteBuf encodeRequest(RpcRequest rpcRequest) throws Exception {
        HuluRpcEncodePacket requestPacket = new HuluRpcEncodePacket();
        HuluRpcProto.HuluRpcRequestMeta.Builder metaBuilder = HuluRpcProto.HuluRpcRequestMeta.newBuilder();
        metaBuilder.setCorrelationId(rpcRequest.getLogId());
        metaBuilder.setLogId(rpcRequest.getLogId());
        int compressType = rpcRequest.getCompressType();
        metaBuilder.setCompressType(compressType);

        // service method
        RpcMetaUtils.RpcMetaInfo rpcMetaInfo = RpcMetaUtils.parseRpcMeta(rpcRequest.getTargetMethod());
        metaBuilder.setServiceName(rpcMetaInfo.getServiceName());
        try {
            int methodIndex = Integer.valueOf(rpcMetaInfo.getMethodName());
            metaBuilder.setMethodIndex(methodIndex);
        } catch (NumberFormatException ex) {
            String errorMsg = "methodName must be integer when using hulu rpc, "
                    + "it is equal to proto method sequence from 0";
            LOG.warn(errorMsg);
            throw new RpcException(RpcException.SERIALIZATION_EXCEPTION, errorMsg);
        }

        // proto
        Object proto = rpcRequest.getArgs()[0];
        Compress compress = compressManager.getCompress(compressType);
        ByteBuf protoBuf = compress.compressInput(proto, rpcRequest.getRpcMethodInfo());
        requestPacket.setProto(protoBuf);

        // attachement
        if (rpcRequest.getBinaryAttachment() != null
                && rpcRequest.getBinaryAttachment().isReadable()) {
            requestPacket.setAttachment(rpcRequest.getBinaryAttachment());
            metaBuilder.setUserMessageSize(protoBuf.readableBytes());
        }
        requestPacket.setRequestMeta(metaBuilder.build());
        ByteBuf outBuf = encode(requestPacket);
        return outBuf;
    }

    @Override
    public RpcResponse decodeResponse(HuluRpcDecodeBasePacket packet, ChannelHandlerContext ctx) throws Exception {
        ByteBuf metaBuf = packet.getMetaBuf();
        ByteBuf protoAndAttachmentBuf = packet.getProtoAndAttachmentBuf();
        ByteBuf protoBuf = null;
        try {
            RpcResponse rpcResponse = new RpcResponse();
            HuluRpcProto.HuluRpcResponseMeta responseMeta = (HuluRpcProto.HuluRpcResponseMeta) ProtobufUtils.parseFrom(
                    metaBuf, defaultRpcResponseMetaInstance);
            Long logId = responseMeta.getCorrelationId();
            rpcResponse.setLogId(logId);

            ChannelInfo channelInfo = ChannelInfo.getClientChannelInfo(ctx.channel());
            RpcFuture future = channelInfo.removeRpcFuture(rpcResponse.getLogId());
            if (future == null) {
                return rpcResponse;
            }
            rpcResponse.setRpcFuture(future);
            int compressType = responseMeta.getCompressType();
            rpcResponse.setCompressType(compressType);
            try {
                if (responseMeta != null && responseMeta.getErrorCode() == 0) {
                    Compress compress = compressManager.getCompress(compressType);
                    if (responseMeta.getUserMessageSize() > 0) {
                        protoBuf = protoAndAttachmentBuf.slice(
                                protoAndAttachmentBuf.readerIndex(),
                                responseMeta.getUserMessageSize());
                    } else {
                        protoBuf = protoAndAttachmentBuf;
                    }
                    Object responseProto = compress.uncompressOutput(protoBuf, future.getRpcMethodInfo());
                    rpcResponse.setResult(responseProto);

                    // attachment
                    if (responseMeta.getUserMessageSize() > 0) {
                        rpcResponse.setBinaryAttachment(protoAndAttachmentBuf);
                        packet.setProtoAndAttachmentBuf(null);
                    }
                } else {
                    rpcResponse.setException(new RpcException(
                            RpcException.SERVICE_EXCEPTION, responseMeta.getErrorText()));
                }
            } catch (Exception ex) {
                // 解析失败直接抛异常
                throw new RpcException(RpcException.SERIALIZATION_EXCEPTION, "decode response failed");
            }
            return rpcResponse;
        } finally {
            if (packet.getMetaBuf() != null) {
                packet.getMetaBuf().release();
            }
            if (packet.getProtoAndAttachmentBuf() != null) {
                packet.getProtoAndAttachmentBuf().release();
            }
        }

    }

    @Override
    public void decodeRequest(HuluRpcDecodeBasePacket packet, RpcRequest rpcRequest) throws Exception {
        ByteBuf metaBuf = packet.getMetaBuf();
        ByteBuf protoAndAttachmentBuf = packet.getProtoAndAttachmentBuf();
        ByteBuf protoBuf = null;
        try {
            HuluRpcProto.HuluRpcRequestMeta requestMeta = (HuluRpcProto.HuluRpcRequestMeta) ProtobufUtils.parseFrom(
                    metaBuf, defaultRpcRequestMetaInstance);
            rpcRequest.setLogId(requestMeta.getCorrelationId());
            int compressType = requestMeta.getCompressType();
            rpcRequest.setCompressType(compressType);

            // service info
            ServiceManager serviceManager = ServiceManager.getInstance();
            RpcMethodInfo rpcMethodInfo = serviceManager.getService(
                    requestMeta.getServiceName(), String.valueOf(requestMeta.getMethodIndex()));
            if (rpcMethodInfo == null) {
                String errorMsg = String.format("Fail to find service=%s, methodIndex=%s",
                        requestMeta.getServiceName(), requestMeta.getMethodIndex());
                rpcRequest.setException(new RpcException(RpcException.SERVICE_EXCEPTION, errorMsg));
                return;
            }
            rpcRequest.setRpcMethodInfo(rpcMethodInfo);
            rpcRequest.setTargetMethod(rpcMethodInfo.getMethod());
            rpcRequest.setTarget(rpcMethodInfo.getTarget());

            // proto body
            try {
                Compress compress = compressManager.getCompress(compressType);
                int userMessageSize = requestMeta.getUserMessageSize();
                if (userMessageSize > 0) {
                    protoBuf = protoAndAttachmentBuf.slice(
                            protoAndAttachmentBuf.readerIndex(),
                            userMessageSize);
                } else {
                    protoBuf = protoAndAttachmentBuf;
                }
                Object requestProto = compress.uncompressInput(protoBuf, rpcMethodInfo);
                rpcRequest.setArgs(new Object[] {requestProto});
                // attachment
                if (userMessageSize > 0) {
                    rpcRequest.setBinaryAttachment(protoAndAttachmentBuf);
                    protoAndAttachmentBuf = null;
                }
            } catch (Exception ex) {
                String errorMsg = String.format("decode failed, msg=%s", ex.getMessage());
                LOG.error(errorMsg);
                throw new RpcException(RpcException.SERIALIZATION_EXCEPTION, errorMsg);
            }
            return;
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
    public ByteBuf encodeResponse(RpcResponse rpcResponse) throws Exception {
        HuluRpcEncodePacket responsePacket = new HuluRpcEncodePacket();
        HuluRpcProto.HuluRpcResponseMeta.Builder metaBuilder = HuluRpcProto.HuluRpcResponseMeta.newBuilder();
        metaBuilder.setCorrelationId(rpcResponse.getLogId());
        int compressType = rpcResponse.getCompressType();
        metaBuilder.setCompressType(compressType);

        if (rpcResponse.getException() != null) {
            metaBuilder.setErrorCode(BaiduRpcErrno.Errno.EINTERNAL_VALUE);
            metaBuilder.setErrorText(rpcResponse.getException().getMessage());
            responsePacket.setResponseMeta(metaBuilder.build());
        } else {
            metaBuilder.setErrorCode(0);
            Compress compress = compressManager.getCompress(compressType);
            ByteBuf responseProtoBuf = compress.compressOutput(
                    rpcResponse.getResult(), rpcResponse.getRpcMethodInfo());
            responsePacket.setProto(responseProtoBuf);

            // attachment
            if (rpcResponse.getBinaryAttachment() != null) {
                responsePacket.setAttachment(rpcResponse.getBinaryAttachment());
                metaBuilder.setUserMessageSize(responseProtoBuf.readableBytes());
            }
            responsePacket.setResponseMeta(metaBuilder.build());
        }

        ByteBuf resBuf = encode(responsePacket);
        return resBuf;
    }

    protected ByteBuf encode(HuluRpcEncodePacket packet) throws IOException {
        int metaSize;
        ByteBuf metaBuf;

        HuluRpcProto.HuluRpcRequestMeta requestMeta = packet.getRequestMeta();
        if (requestMeta != null) {
            // request
            byte[] metaBytes = requestMeta.toByteArray();
            metaSize = metaBytes.length;
            metaBuf = Unpooled.wrappedBuffer(metaBytes);
        } else {
            // response
            byte[] metaBytes = packet.getResponseMeta().toByteArray();
            metaSize = metaBytes.length;
            metaBuf = Unpooled.wrappedBuffer(metaBytes);
        }

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
        headerBuf.writeIntLE(bodySize);
        headerBuf.writeIntLE(metaSize);

        if (protoBuf != null && attachmentBuf != null) {
            return Unpooled.wrappedBuffer(headerBuf, metaBuf, protoBuf, attachmentBuf);
        } else if (protoBuf != null) {
            return Unpooled.wrappedBuffer(headerBuf, metaBuf, protoBuf);
        } else {
            return  Unpooled.wrappedBuffer(headerBuf, metaBuf);
        }
    }

    public HuluRpcDecodeBasePacket decode(DynamicCompositeByteBuf in)
            throws BadSchemaException, TooBigDataException, NotEnoughDataException {
        if (in.readableBytes() < FIXED_LEN) {
            throw new NotEnoughDataException("readable bytes less than 12 for hulu:" + in.readableBytes());
        }
        ByteBuf fixHeaderBuf = in.retainedSlice(FIXED_LEN);

        try {
            byte[] magic = new byte[4];
            fixHeaderBuf.readBytes(magic);
            if (!Arrays.equals(magic, MAGIC_HEAD)) {
                throw new BadSchemaException("not valid magic head for hulu");
            }
            int bodySize = fixHeaderBuf.readIntLE();
            int metaSize = fixHeaderBuf.readIntLE();
            // 512M
            if (bodySize > 512 * 1024 * 1024) {
                throw new TooBigDataException("to big body size:" + bodySize);
            }
            if (in.readableBytes() < FIXED_LEN + bodySize) {
                String errMsg = String.format("readable bytes=%d, bodySize=%d", in.readableBytes(), bodySize);
                throw new NotEnoughDataException(errMsg);
            }

            in.skipBytes(FIXED_LEN);
            HuluRpcDecodeBasePacket packet = new HuluRpcDecodeBasePacket();
            try {
                // meta
                ByteBuf metaBuf = in.readRetainedSlice(metaSize);
                packet.setMetaBuf(metaBuf);

                // body
                ByteBuf protoAndAttachmentBuf = in.readRetainedSlice(bodySize - metaSize);
                packet.setProtoAndAttachmentBuf(protoAndAttachmentBuf);

                return packet;
            } catch (Exception ex) {
                LOG.warn("decode failed, ex={}", ex.getMessage());
                throw new RpcException(RpcException.SERIALIZATION_EXCEPTION, ex);
            }
        } finally {
            fixHeaderBuf.release();
        }
    }
}
