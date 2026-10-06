package com.minikafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Partition {
    private List<LogSegment> logSegments;
    private int position;
    private int maxBytes;
    private String topic;
    private Path dir;

    public Partition(Path dir, String topic, int position, int maxBytes) throws IOException {
        this.logSegments = new ArrayList<>();
        Files.createDirectories(dir);
        LogSegment logSegment = new LogSegment(dir, 0, maxBytes);
        this.logSegments.add(logSegment);
        this.position = position;
        this.maxBytes = maxBytes;
        this.topic = topic;
        this.dir = dir;
    }

    public synchronized long append(List<Record> batch) throws IOException{
        
        long offset = -1; 
        for (Record r: batch) {
            if (r.serializedSize() > maxBytes) {
                throw new IllegalArgumentException("record exceeds maxBytes: " + r.serializedSize() + " > " + maxBytes);
            }   
        }

        for (Record r: batch) {
            LogSegment segment = logSegments.getLast();
            long recordOffset = segment.append(r);
            long assignedOffset = maybeRoll(recordOffset, r, segment); 
            if (offset == -1) {
                offset = assignedOffset;
            }     
        }
        logSegments.getLast().flush();
        return offset;
    }
    // not yet implemented
    // public synchronized FetchResult read(long offset, int maxBytes) {
    //     return null;
    // }
    
    public long earliestOffset() {
        return logSegments.getFirst().getBaseOffset(); 
    }

    public long latestOffset() {
        return logSegments.getLast().getNextOffset();
    }

    private long maybeRoll(long offset, Record record,LogSegment segment) throws IOException {
        long newOffset = offset;
        if (newOffset == -1) {
            long nextOffset = segment.getNextOffset();
            segment.flush();
            segment = new LogSegment(this.dir, nextOffset, this.maxBytes);
            newOffset = segment.append(record);
            logSegments.add(segment);
        }
        return newOffset;
    }
}
