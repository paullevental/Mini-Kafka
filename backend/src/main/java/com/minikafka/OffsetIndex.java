package com.minikafka;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

// class for representing indexing and location in storage
public class OffsetIndex {

    private MappedByteBuffer buffer;
    private int entryCount = 0;
    private long baseOffset;
    private int interval;
    private FileChannel fileChannel;
    private int maxEntries;
    private int lastIndexedPosition = 0;


    public OffsetIndex(Path dir, int maxBytes, long baseOffset, int interval) throws IOException {
        Path file = dir.resolve(String.format("%020d.index", baseOffset));
        this.maxEntries = (maxBytes / interval);
        this.fileChannel = FileChannel.open(file,StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        this.entryCount = (int) (fileChannel.size() / 8);
        this.buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxEntries * 8);
        this.lastIndexedPosition = entryCount > 0 ? buffer.getInt((entryCount - 1) * 8 + 4) : 0;
        this.baseOffset = baseOffset;
        this.interval = interval;
    }

    
    public void maybeAppend(long offset, int position) {
        int delta = (int) (offset - baseOffset);
        if (entryCount < maxEntries && position - lastIndexedPosition >= interval) {
            buffer.putInt(entryCount * 8, delta);
            buffer.putInt(entryCount * 8 + 4, position);
            entryCount++;
            lastIndexedPosition = position;
        }
    }


    public int lookup(long offset) {
        int low = 0;
        int high = entryCount - 1;
        int delta = (int) (offset - this.baseOffset);

        while (low <= high) {
            int mid = low + (high - low) / 2;
            int midDelta = buffer.getInt(mid * 8);

            if (delta >= midDelta) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        if (high >= 0) {
            return buffer.getInt(high * 8 + 4);    
        }
        return 0;  
    }
    
    public void resizeOnClose() throws IOException {
        buffer.force();
        fileChannel.truncate(entryCount * 8L);
        fileChannel.close();
    }
}
