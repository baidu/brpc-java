/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 * Copy form Netty project
 */

package com.baidu.brpc.protocol.grpc.client;

import com.baidu.brpc.protocol.grpc.client.Http2CodecUtil.SimpleChannelPromiseAggregator;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.*;
import io.netty.handler.codec.http2.Http2HeadersEncoder.SensitivityDetector;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.UnstableApi;

import static io.netty.buffer.Unpooled.directBuffer;
import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static io.netty.handler.codec.http2.Http2CodecUtil.*;
import static io.netty.handler.codec.http2.Http2Error.FRAME_SIZE_ERROR;
import static io.netty.handler.codec.http2.Http2Exception.connectionError;
import static io.netty.handler.codec.http2.Http2FrameTypes.*;
import static io.netty.util.internal.ObjectUtil.*;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * A {@link Http2FrameWriter} that supports all frame types defined by the HTTP/2 specification.
 */
@UnstableApi
public class Http2GrpcFrameWriter implements Http2FrameSizePolicy {
    private static final String STREAM_ID = "Stream ID";
    private static final String STREAM_DEPENDENCY = "Stream Dependency";
    /**
     * This buffer is allocated to the maximum size of the padding field, and filled with zeros.
     * When padding is needed it can be taken as a slice of this buffer. Users should call {@link ByteBuf#retain()}
     * before using their slice.
     */
    private static final ByteBuf ZERO_BUFFER =
            unreleasableBuffer(directBuffer(MAX_UNSIGNED_BYTE).writeZero(MAX_UNSIGNED_BYTE)).asReadOnly();

    private final Http2HeadersEncoder headersEncoder;
    private int maxFrameSize;

    public Http2GrpcFrameWriter() {
        this(new DefaultHttp2HeadersEncoder());
    }

    public Http2GrpcFrameWriter(SensitivityDetector headersSensitivityDetector) {
        this(new DefaultHttp2HeadersEncoder(headersSensitivityDetector));
    }

    public Http2GrpcFrameWriter(SensitivityDetector headersSensitivityDetector, boolean ignoreMaxHeaderListSize) {
        this(new DefaultHttp2HeadersEncoder(headersSensitivityDetector, ignoreMaxHeaderListSize));
    }

    public Http2GrpcFrameWriter(Http2HeadersEncoder headersEncoder) {
        this.headersEncoder = headersEncoder;
        maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
    }

    @Override
    public void maxFrameSize(int max) throws Http2Exception {
        if (!isMaxFrameSizeValid(max)) {
            throw connectionError(FRAME_SIZE_ERROR, "Invalid MAX_FRAME_SIZE specified in sent settings: %d", max);
        }
        maxFrameSize = max;
    }

    @Override
    public int maxFrameSize() {
        return maxFrameSize;
    }

    public ChannelFuture writeData(Channel channel, int streamId, ByteBuf data,
                                   int padding, boolean endStream, ChannelPromise promise) {
        final SimpleChannelPromiseAggregator promiseAggregator =
                new SimpleChannelPromiseAggregator(promise, channel,channel.eventLoop());
        ByteBuf frameHeader = null;
        try {
            verifyStreamId(streamId, STREAM_ID);
            verifyPadding(padding);

            int remainingData = data.readableBytes();
            Http2Flags flags = new Http2Flags();
            flags.endOfStream(false);
            flags.paddingPresent(false);
            // Fast path to write frames of payload size maxFrameSize first.
            if (remainingData > maxFrameSize) {
                frameHeader = channel.alloc().buffer(FRAME_HEADER_LENGTH);
                Http2CodecUtil.writeFrameHeaderInternal(frameHeader, maxFrameSize, DATA, flags, streamId);
                do {
                    // Write the header.
                    channel.write(frameHeader.retainedSlice(), promiseAggregator.newPromise());

                    // Write the payload.
                    channel.write(data.readRetainedSlice(maxFrameSize), promiseAggregator.newPromise());

                    remainingData -= maxFrameSize;
                    // Stop iterating if remainingData == maxFrameSize so we can take care of reference counts below.
                } while (remainingData > maxFrameSize);
            }

            if (padding == 0) {
                // Write the header.
                if (frameHeader != null) {
                    frameHeader.release();
                    frameHeader = null;
                }
                ByteBuf frameHeader2 = channel.alloc().buffer(FRAME_HEADER_LENGTH);
                flags.endOfStream(endStream);
                Http2CodecUtil.writeFrameHeaderInternal(frameHeader2, remainingData, DATA, flags, streamId);
                channel.write(frameHeader2, promiseAggregator.newPromise());

                // Write the payload.
                ByteBuf lastFrame = data.readSlice(remainingData);
                data = null;
                channel.write(lastFrame, promiseAggregator.newPromise());
            } else {
                if (remainingData != maxFrameSize) {
                    if (frameHeader != null) {
                        frameHeader.release();
                        frameHeader = null;
                    }
                } else {
                    remainingData -= maxFrameSize;
                    // Write the header.
                    ByteBuf lastFrame;
                    if (frameHeader == null) {
                        lastFrame = channel.alloc().buffer(FRAME_HEADER_LENGTH);
                        Http2CodecUtil.writeFrameHeaderInternal(lastFrame, maxFrameSize, DATA, flags, streamId);
                    } else {
                        lastFrame = frameHeader.slice();
                        frameHeader = null;
                    }
                    channel.write(lastFrame, promiseAggregator.newPromise());

                    // Write the payload.
                    lastFrame = data.readableBytes() != maxFrameSize ? data.readSlice(maxFrameSize) : data;
                    data = null;
                    channel.write(lastFrame, promiseAggregator.newPromise());
                }

                do {
                    int frameDataBytes = min(remainingData, maxFrameSize);
                    int framePaddingBytes = min(padding, max(0, (maxFrameSize - 1) - frameDataBytes));

                    // Decrement the remaining counters.
                    padding -= framePaddingBytes;
                    remainingData -= frameDataBytes;

                    // Write the header.
                    ByteBuf frameHeader2 = channel.alloc().buffer(DATA_FRAME_HEADER_LENGTH);
                    flags.endOfStream(endStream && remainingData == 0 && padding == 0);
                    flags.paddingPresent(framePaddingBytes > 0);
                    Http2CodecUtil.writeFrameHeaderInternal(frameHeader2, framePaddingBytes + frameDataBytes, DATA, flags, streamId);
                    writePaddingLength(frameHeader2, framePaddingBytes);
                    channel.write(frameHeader2, promiseAggregator.newPromise());

                    // Write the payload.
                    if (frameDataBytes != 0) {
                        if (remainingData == 0) {
                            ByteBuf lastFrame = data.readSlice(frameDataBytes);
                            data = null;
                            channel.write(lastFrame, promiseAggregator.newPromise());
                        } else {
                            channel.write(data.readRetainedSlice(frameDataBytes), promiseAggregator.newPromise());
                        }
                    }
                    // Write the frame padding.
                    if (paddingBytes(framePaddingBytes) > 0) {
                        channel.write(ZERO_BUFFER.slice(0, paddingBytes(framePaddingBytes)),
                                  promiseAggregator.newPromise());
                    }
                } while (remainingData != 0 || padding != 0);
            }
        } catch (Throwable cause) {
            if (frameHeader != null) {
                frameHeader.release();
            }
            // Use a try/finally here in case the data has been released before calling this method. This is not
            // necessary above because we internally allocate frameHeader.
            try {
                if (data != null) {
                    data.release();
                }
            } finally {
                promiseAggregator.setFailure(cause);
                promiseAggregator.doneAllocatingPromises();
            }
            return promiseAggregator;
        }
        return promiseAggregator.doneAllocatingPromises();
    }

    public ChannelFuture writeHeaders(Channel ctx, int streamId,
            Http2Headers headers, int padding, boolean endStream, ChannelPromise promise) {
        return writeHeadersInternal(ctx, streamId, headers, padding, endStream,
                false, 0, (short) 0, false, promise);
    }

    public ChannelFuture writeHeaders(Channel ctx, int streamId,
            Http2Headers headers, int streamDependency, short weight, boolean exclusive,
            int padding, boolean endStream, ChannelPromise promise) {
        return writeHeadersInternal(ctx, streamId, headers, padding, endStream,
                true, streamDependency, weight, exclusive, promise);
    }

    public ChannelFuture writePriority(Channel ctx, int streamId,
            int streamDependency, short weight, boolean exclusive, ChannelPromise promise) {
        try {
            verifyStreamId(streamId, STREAM_ID);
            verifyStreamId(streamDependency, STREAM_DEPENDENCY);
            verifyWeight(weight);

            ByteBuf buf = ctx.alloc().buffer(PRIORITY_FRAME_LENGTH);
            Http2CodecUtil.writeFrameHeaderInternal(buf, PRIORITY_ENTRY_LENGTH, PRIORITY, new Http2Flags(), streamId);
            buf.writeInt(exclusive ? (int) (0x80000000L | streamDependency) : streamDependency);
            // Adjust the weight so that it fits into a single byte on the wire.
            buf.writeByte(weight - 1);
            return ctx.write(buf, promise);
        } catch (Throwable t) {
            return promise.setFailure(t);
        }
    }

    public ChannelFuture writeRstStream(Channel ctx, int streamId, long errorCode,
            ChannelPromise promise) {
        try {
            verifyStreamId(streamId, STREAM_ID);
            verifyErrorCode(errorCode);

            ByteBuf buf = ctx.alloc().buffer(RST_STREAM_FRAME_LENGTH);
            Http2CodecUtil.writeFrameHeaderInternal(buf, INT_FIELD_LENGTH, RST_STREAM, new Http2Flags(), streamId);
            buf.writeInt((int) errorCode);
            return ctx.write(buf, promise);
        } catch (Throwable t) {
            return promise.setFailure(t);
        }
    }

    public ChannelFuture writeSettings(Channel ctx, Http2Settings settings,
            ChannelPromise promise) {
        try {
            checkNotNull(settings, "settings");
            int payloadLength = SETTING_ENTRY_LENGTH * settings.size();
            ByteBuf buf = ctx.alloc().buffer(FRAME_HEADER_LENGTH + settings.size() * SETTING_ENTRY_LENGTH);
            Http2CodecUtil.writeFrameHeaderInternal(buf, payloadLength, SETTINGS, new Http2Flags(), 0);
            for (Http2Settings.PrimitiveEntry<Long> entry : settings.entries()) {
                buf.writeChar(entry.key());
                buf.writeInt(entry.value().intValue());
            }
            return ctx.write(buf, promise);
        } catch (Throwable t) {
            return promise.setFailure(t);
        }
    }

    public ChannelFuture writeSettingsAck(Channel ctx, ChannelPromise promise) {
        try {
            ByteBuf buf = ctx.alloc().buffer(FRAME_HEADER_LENGTH);
            Http2CodecUtil.writeFrameHeaderInternal(buf, 0, SETTINGS, new Http2Flags().ack(true), 0);
            return ctx.write(buf, promise);
        } catch (Throwable t) {
            return promise.setFailure(t);
        }
    }

    public ChannelFuture writePing(Channel ctx, boolean ack, long data, ChannelPromise promise) {
        Http2Flags flags = ack ? new Http2Flags().ack(true) : new Http2Flags();
        ByteBuf buf = ctx.alloc().buffer(FRAME_HEADER_LENGTH + PING_FRAME_PAYLOAD_LENGTH);
        // Assume nothing below will throw until buf is written. That way we don't have to take care of ownership
        // in the catch block.
        Http2CodecUtil.writeFrameHeaderInternal(buf, PING_FRAME_PAYLOAD_LENGTH, PING, flags, 0);
        buf.writeLong(data);
        return ctx.write(buf, promise);
    }

    public ChannelFuture writePushPromise(Channel channel, int streamId,
            int promisedStreamId, Http2Headers headers, int padding, ChannelPromise promise) {
        ByteBuf headerBlock = null;
        SimpleChannelPromiseAggregator promiseAggregator =
                new SimpleChannelPromiseAggregator(promise, channel, channel.eventLoop());
        try {
            verifyStreamId(streamId, STREAM_ID);
            verifyStreamId(promisedStreamId, "Promised Stream ID");
            verifyPadding(padding);

            // Encode the entire header block into an intermediate buffer.
            headerBlock = channel.alloc().buffer();
            headersEncoder.encodeHeaders(streamId, headers, headerBlock);

            // Read the first fragment (possibly everything).
            Http2Flags flags = new Http2Flags().paddingPresent(padding > 0);
            // INT_FIELD_LENGTH is for the length of the promisedStreamId
            int nonFragmentLength = INT_FIELD_LENGTH + padding;
            int maxFragmentLength = maxFrameSize - nonFragmentLength;
            ByteBuf fragment = headerBlock.readRetainedSlice(min(headerBlock.readableBytes(), maxFragmentLength));

            flags.endOfHeaders(!headerBlock.isReadable());

            int payloadLength = fragment.readableBytes() + nonFragmentLength;
            ByteBuf buf = channel.alloc().buffer(PUSH_PROMISE_FRAME_HEADER_LENGTH);
            Http2CodecUtil.writeFrameHeaderInternal(buf, payloadLength, PUSH_PROMISE, flags, streamId);
            writePaddingLength(buf, padding);

            // Write out the promised stream ID.
            buf.writeInt(promisedStreamId);
            channel.write(buf, promiseAggregator.newPromise());

            // Write the first fragment.
            channel.write(fragment, promiseAggregator.newPromise());

            // Write out the padding, if any.
            if (paddingBytes(padding) > 0) {
                channel.write(ZERO_BUFFER.slice(0, paddingBytes(padding)), promiseAggregator.newPromise());
            }

            if (!flags.endOfHeaders()) {
                writeContinuationFrames(channel, streamId, headerBlock, promiseAggregator);
            }
        } catch (Http2Exception e) {
            promiseAggregator.setFailure(e);
        } catch (Throwable t) {
            promiseAggregator.setFailure(t);
            promiseAggregator.doneAllocatingPromises();
            PlatformDependent.throwException(t);
        } finally {
            if (headerBlock != null) {
                headerBlock.release();
            }
        }
        return promiseAggregator.doneAllocatingPromises();
    }

    public ChannelFuture writeGoAway(Channel channel, int lastStreamId, long errorCode,
            ByteBuf debugData, ChannelPromise promise) {
        SimpleChannelPromiseAggregator promiseAggregator =
                new SimpleChannelPromiseAggregator(promise, channel, channel.eventLoop());
        try {
            verifyStreamOrConnectionId(lastStreamId, "Last Stream ID");
            verifyErrorCode(errorCode);

            int payloadLength = 8 + debugData.readableBytes();
            ByteBuf buf = channel.alloc().buffer(GO_AWAY_FRAME_HEADER_LENGTH);
            // Assume nothing below will throw until buf is written. That way we don't have to take care of ownership
            // in the catch block.
            Http2CodecUtil.writeFrameHeaderInternal(buf, payloadLength, GO_AWAY, new Http2Flags(), 0);
            buf.writeInt(lastStreamId);
            buf.writeInt((int) errorCode);
            channel.write(buf, promiseAggregator.newPromise());
        } catch (Throwable t) {
            try {
                debugData.release();
            } finally {
                promiseAggregator.setFailure(t);
                promiseAggregator.doneAllocatingPromises();
            }
            return promiseAggregator;
        }

        try {
            channel.write(debugData, promiseAggregator.newPromise());
        } catch (Throwable t) {
            promiseAggregator.setFailure(t);
        }
        return promiseAggregator.doneAllocatingPromises();
    }

    public ChannelFuture writeWindowUpdate(Channel ctx, int streamId,
            int windowSizeIncrement, ChannelPromise promise) {
        try {
            verifyStreamOrConnectionId(streamId, STREAM_ID);
            verifyWindowSizeIncrement(windowSizeIncrement);

            ByteBuf buf = ctx.alloc().buffer(WINDOW_UPDATE_FRAME_LENGTH);
            Http2CodecUtil.writeFrameHeaderInternal(buf, INT_FIELD_LENGTH, WINDOW_UPDATE, new Http2Flags(), streamId);
            buf.writeInt(windowSizeIncrement);
            return ctx.write(buf, promise);
        } catch (Throwable t) {
            return promise.setFailure(t);
        }
    }

    public ChannelFuture writeFrame(Channel channel, byte frameType, int streamId,
            Http2Flags flags, ByteBuf payload, ChannelPromise promise) {
        SimpleChannelPromiseAggregator promiseAggregator =
                new SimpleChannelPromiseAggregator(promise, channel, channel.eventLoop());
        try {
            verifyStreamOrConnectionId(streamId, STREAM_ID);
            ByteBuf buf = channel.alloc().buffer(FRAME_HEADER_LENGTH);
            // Assume nothing below will throw until buf is written. That way we don't have to take care of ownership
            // in the catch block.
            Http2CodecUtil.writeFrameHeaderInternal(buf, payload.readableBytes(), frameType, flags, streamId);
            channel.write(buf, promiseAggregator.newPromise());
        } catch (Throwable t) {
            try {
                payload.release();
            } finally {
                promiseAggregator.setFailure(t);
                promiseAggregator.doneAllocatingPromises();
            }
            return promiseAggregator;
        }
        try {
            channel.write(payload, promiseAggregator.newPromise());
        } catch (Throwable t) {
            promiseAggregator.setFailure(t);
        }
        return promiseAggregator.doneAllocatingPromises();
    }

    private ChannelFuture writeHeadersInternal(Channel channel,
            int streamId, Http2Headers headers, int padding, boolean endStream,
            boolean hasPriority, int streamDependency, short weight, boolean exclusive, ChannelPromise promise) {
        ByteBuf headerBlock = null;
        SimpleChannelPromiseAggregator promiseAggregator =
                new SimpleChannelPromiseAggregator(promise, channel, channel.eventLoop());
        try {
            verifyStreamId(streamId, STREAM_ID);
            if (hasPriority) {
                verifyStreamOrConnectionId(streamDependency, STREAM_DEPENDENCY);
                verifyPadding(padding);
                verifyWeight(weight);
            }

            // Encode the entire header block.
            headerBlock = channel.alloc().buffer();
            headersEncoder.encodeHeaders(streamId, headers, headerBlock);

            Http2Flags flags =
                    new Http2Flags().endOfStream(endStream).priorityPresent(hasPriority).paddingPresent(padding > 0);

            // Read the first fragment (possibly everything).
            int nonFragmentBytes = padding + flags.getNumPriorityBytes();
            int maxFragmentLength = maxFrameSize - nonFragmentBytes;
            ByteBuf fragment = headerBlock.readRetainedSlice(min(headerBlock.readableBytes(), maxFragmentLength));

            // Set the end of headers flag for the first frame.
            flags.endOfHeaders(!headerBlock.isReadable());

            int payloadLength = fragment.readableBytes() + nonFragmentBytes;
            ByteBuf buf = channel.alloc().buffer(HEADERS_FRAME_HEADER_LENGTH);
            Http2CodecUtil.writeFrameHeaderInternal(buf, payloadLength, HEADERS, flags, streamId);
            writePaddingLength(buf, padding);

            if (hasPriority) {
                buf.writeInt(exclusive ? (int) (0x80000000L | streamDependency) : streamDependency);

                // Adjust the weight so that it fits into a single byte on the wire.
                buf.writeByte(weight - 1);
            }
            channel.write(buf, promiseAggregator.newPromise());

            // Write the first fragment.
            channel.write(fragment, promiseAggregator.newPromise());

            // Write out the padding, if any.
            if (paddingBytes(padding) > 0) {
                channel.write(ZERO_BUFFER.slice(0, paddingBytes(padding)), promiseAggregator.newPromise());
            }

            if (!flags.endOfHeaders()) {
                writeContinuationFrames(channel, streamId, headerBlock, promiseAggregator);
            }
        } catch (Http2Exception e) {
            promiseAggregator.setFailure(e);
        } catch (Throwable t) {
            promiseAggregator.setFailure(t);
            promiseAggregator.doneAllocatingPromises();
            PlatformDependent.throwException(t);
        } finally {
            if (headerBlock != null) {
                headerBlock.release();
            }
        }
        return promiseAggregator.doneAllocatingPromises();
    }

    /**
     * Writes as many continuation frames as needed until {@code padding} and {@code headerBlock} are consumed.
     */
    private ChannelFuture writeContinuationFrames(Channel ctx, int streamId,
            ByteBuf headerBlock, SimpleChannelPromiseAggregator promiseAggregator) {
        Http2Flags flags = new Http2Flags();

        if (headerBlock.isReadable()) {
            // The frame header (and padding) only changes on the last frame, so allocate it once and re-use
            int fragmentReadableBytes = min(headerBlock.readableBytes(), maxFrameSize);
            ByteBuf buf = ctx.alloc().buffer(CONTINUATION_FRAME_HEADER_LENGTH);
            Http2CodecUtil.writeFrameHeaderInternal(buf, fragmentReadableBytes, CONTINUATION, flags, streamId);

            do {
                fragmentReadableBytes = min(headerBlock.readableBytes(), maxFrameSize);
                ByteBuf fragment = headerBlock.readRetainedSlice(fragmentReadableBytes);

                if (headerBlock.isReadable()) {
                    ctx.write(buf.retain(), promiseAggregator.newPromise());
                } else {
                    // The frame header is different for the last frame, so re-allocate and release the old buffer
                    flags = flags.endOfHeaders(true);
                    buf.release();
                    buf = ctx.alloc().buffer(CONTINUATION_FRAME_HEADER_LENGTH);
                    Http2CodecUtil.writeFrameHeaderInternal(buf, fragmentReadableBytes, CONTINUATION, flags, streamId);
                    ctx.write(buf, promiseAggregator.newPromise());
                }

                ctx.write(fragment, promiseAggregator.newPromise());

            } while (headerBlock.isReadable());
        }
        return promiseAggregator;
    }

    /**
     * Returns the number of padding bytes that should be appended to the end of a frame.
     */
    private static int paddingBytes(int padding) {
        // The padding parameter contains the 1 byte pad length field as well as the trailing padding bytes.
        // Subtract 1, so to only get the number of padding bytes that need to be appended to the end of a frame.
        return padding - 1;
    }

    private static void writePaddingLength(ByteBuf buf, int padding) {
        if (padding > 0) {
            // It is assumed that the padding length has been bounds checked before this
            // Minus 1, as the pad length field is included in the padding parameter and is 1 byte wide.
            buf.writeByte(padding - 1);
        }
    }

    private static void verifyStreamId(int streamId, String argumentName) {
        checkPositive(streamId, "streamId");
    }

    private static void verifyStreamOrConnectionId(int streamId, String argumentName) {
        checkPositiveOrZero(streamId, "streamId");
    }

    private static void verifyWeight(short weight) {
        if (weight < MIN_WEIGHT || weight > MAX_WEIGHT) {
            throw new IllegalArgumentException("Invalid weight: " + weight);
        }
    }

    private static void verifyErrorCode(long errorCode) {
        if (errorCode < 0 || errorCode > MAX_UNSIGNED_INT) {
            throw new IllegalArgumentException("Invalid errorCode: " + errorCode);
        }
    }

    private static void verifyWindowSizeIncrement(int windowSizeIncrement) {
        checkPositiveOrZero(windowSizeIncrement, "windowSizeIncrement");
    }

    private static void verifyPingPayload(ByteBuf data) {
        if (data == null || data.readableBytes() != PING_FRAME_PAYLOAD_LENGTH) {
            throw new IllegalArgumentException("Opaque data must be " + PING_FRAME_PAYLOAD_LENGTH + " bytes");
        }
    }
}