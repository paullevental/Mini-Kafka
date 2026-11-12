package com.minikafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Partition {
    private List<LogSegment> logSegments;
    private int maxBytes;
    private Path dir;

    public Partition(Path dir, int maxBytes) throws IOException {
        this.logSegments = new ArrayList<>();
        Files.createDirectories(dir);
        LogSegment logSegment = new LogSegment(dir, 0, maxBytes);
        this.logSegments.add(logSegment);
        this.maxBytes = maxBytes;
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

    public synchronized FetchResult read(long offset, int maxFetchBytes) throws IOException {
        
        LogSegment readSegment = null;
        
        for (LogSegment segment : logSegments) {
            if ((segment.getBaseOffset()) <= offset) {
                readSegment = segment;
            }
        }
        if (readSegment == null) {
            throw new IllegalArgumentException("no valid offset found");
        }   

        List<Record> records = readSegment.read(offset, maxFetchBytes);
        FetchResult result = new FetchResult(records, latestOffset());  
        return result;
    }
    
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

    public void close() {
        for (LogSegment logSegment : logSegments) {
            try {
                logSegment.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
