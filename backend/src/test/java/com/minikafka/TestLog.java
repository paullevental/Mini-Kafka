package com.minikafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestLog {

    @Test
    void testReadAll(@TempDir Path tempPath) throws IOException{
        byte[] payload1 = {1,2,3};
        byte[] payload2 = {4,5,6};

        
        Log log = new Log(tempPath.resolve("test.log"));
        log.append(payload1);
        log.append(payload2);
        System.out.println(log.readAll());
        assertArrayEquals(log.readAll(), ByteBuffer.allocate(payload1.length + payload2.length)
                            .put(payload1)
                            .put(payload2)
                            .array());
    }

    
}
