package com.minikafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;


// But the flat-file storage. Append bytes, read all bytes.
// No offsets, no record boundaries. LogSegment replaces this Day 2.
public class Log {

    private final Path file;

    public Log(Path file) throws IOException{
        this.file = file;
        Files.createDirectories(file.getParent());
    }

    public synchronized void append(byte[] payload) throws IOException { 

        Files.write(file, payload,StandardOpenOption.CREATE, 
                        StandardOpenOption.APPEND);

    }

    public synchronized byte[] readAll() throws IOException { 
        if (!(Files.exists(file))) {
            return new byte[0];
        }
        return Files.readAllBytes(file);
    }
    
}
