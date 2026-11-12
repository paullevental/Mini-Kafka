package com.minikafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestPartition {


    @Test
    void testPartitionAppendAndRead(@TempDir Path tempPath)  {
        try {
            Partition p = new Partition(tempPath, 1024);
            List<Record> batch = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                byte[] value = new byte[]{(byte) (i*2)};
                byte[] key = new byte[] {(byte) i};
                Record r = new Record(i, i, key, value);
                batch.add(r);
            }
            assertEquals(0,  p.append(batch));
            assertEquals(0,  p.earliestOffset());
            assertEquals(50, p.latestOffset());

            // budget allows 100 records, but segment 0 holds 30
            FetchResult a = p.read(0, 3400);
            assertEquals(30, a.records().size());
            assertEquals(0,  a.records().getFirst().offset());
            assertEquals(29, a.records().getLast().offset());
            assertEquals(50, a.endOffset());          

            FetchResult b = p.read(10, 112);
            assertEquals(4,  b.records().size());     
            assertEquals(10, b.records().getFirst().offset());
            assertEquals(13, b.records().getLast().offset());

            FetchResult c = p.read(35, 100);         
            assertEquals(35, c.records().getFirst().offset());
            assertEquals(50, c.endOffset());

            assertEquals(0,  p.read(50, 1000).records().size());  

            assertThrows(IllegalArgumentException.class, () -> p.read(-5, 100));
            assertThrows(IllegalArgumentException.class, () -> p.read(99, 100));
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
}
