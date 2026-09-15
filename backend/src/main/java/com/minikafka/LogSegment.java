package com.minikafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

// a class for one files worth of storage
public class LogSegment {

    private long baseOffset;    
    private int maxBytes;
    private FileChannel fileChannel;
    private int position;
    private OffsetIndex index;
    private static final int INTERVAL = 4096;
    private long nextOffset;


    public LogSegment(Path dir, long baseOffset, int maxBytes) throws IOException {
        this.baseOffset = baseOffset;
        this.nextOffset = baseOffset;
        this.maxBytes = maxBytes;
        Path file = dir.resolve(String.format("%020d.log", baseOffset));
        this.fileChannel = FileChannel.open(file,StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.READ);
        this.position = (int) fileChannel.size();
        this.index = new OffsetIndex(dir, maxBytes, baseOffset, INTERVAL);
    }

    public synchronized long append(Record r) throws IOException{
        int startPosition = (int) position;
        if (startPosition + r.serializedSize() > maxBytes) return -1;
        long assignedOffset = nextOffset;
        nextOffset++;
        Record stampRecord = new Record(assignedOffset, System.currentTimeMillis(), r.key(), r.value());
        byte[] bytes = stampRecord.serialize();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        fileChannel.write(buffer, startPosition);
        index.maybeAppend(assignedOffset, startPosition);
        position += bytes.length;
        return assignedOffset;
    }


    public List<Record> read(long startOffset, int maxFetchBytes) throws IOException {
        int startPosition = 0;
        if (startOffset >= baseOffset && startOffset <= nextOffset) {
            startPosition = index.lookup(startOffset);
        } else {
            throw new IllegalArgumentException("start offset out of range");
        }

        int length = Math.min(maxFetchBytes + INTERVAL, position - startPosition);
        ByteBuffer buffer = ByteBuffer.allocate(length);
        fileChannel.read(buffer, startPosition);
        buffer.flip();
        ArrayList<Record> records = new ArrayList<>();
        int bytesCollected = 0;
        while (true) {
            if (buffer.remaining() < 4) break;
            int len = buffer.getInt(buffer.position());
            if (buffer.remaining() < 4 + len) break;
            if (bytesCollected >= maxFetchBytes) break;
            Record r = Record.deserialize(buffer);
            if (r.offset() >= startOffset) {
                records.add(r);
                bytesCollected += r.serializedSize();
            } 
        }
        return records;
    }

    public int sizeInBytes() {
        return position;
    }

    public void flush() throws IOException {
        fileChannel.force(false);
    }      
                            
    public void close() throws IOException {
        flush();
        index.resizeOnClose();
        fileChannel.close();
    }

    public long recoverAndTruncate() throws IOException {
        return nextOffset;
    }         

    
}