package io.github.tuyucheng777.websocket.frame;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/// Accumulator for fragmented message payloads.
///
/// A single logical WebSocket message may be carried by multiple frames and may span multiple TCP reads.
/// The accumulator collects already-unmasked bytes in a list of chunks and performs only a single linear
/// copy in {@link #drain()}, avoiding repeated reallocation and copying during growth.
final class MessageAccumulator {

    private final ArrayList<byte[]> chunks = new ArrayList<>();
    private long totalLength;

    /// Appends a segment of bytes that have already been unmasked.
    ///
    /// @param source source buffer (its remaining bytes are read and the position advances)
    void append(ByteBuffer source) {
        int remaining = source.remaining();
        if (remaining == 0) {
            return;
        }
        byte[] block = new byte[remaining];
        source.get(block);
        chunks.add(block);
        totalLength += remaining;
    }

    /// Appends an external array.
    ///
    /// @param data bytes that have already been unmasked
    void append(byte[] data) {
        if (data.length == 0) {
            return;
        }
        chunks.add(data);
        totalLength += data.length;
    }

    /// Returns the total number of accumulated bytes.
    ///
    /// @return total number of bytes
    long size() {
        return totalLength;
    }

    /// Determines whether the accumulator is empty.
    ///
    /// @return {@code true} when there are no bytes at all
    boolean isEmpty() {
        return totalLength == 0;
    }

    /// Retrieves the accumulated result and clears the state; the accumulator can be reused after this call.
    ///
    /// @return a new array containing all accumulated bytes
    byte[] drain() {
        byte[] result;
        if (chunks.size() == 1) {
            result = chunks.getFirst();
        } else {
            result = new byte[(int) totalLength];
            int offset = 0;
            for (byte[] block : chunks) {
                System.arraycopy(block, 0, result, offset, block.length);
                offset += block.length;
            }
        }
        chunks.clear();
        totalLength = 0;
        return result;
    }
}
