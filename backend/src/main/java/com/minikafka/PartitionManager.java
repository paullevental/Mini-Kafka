package com.minikafka;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionManager {

    private final ConcurrentHashMap<TopicPartition, Partition> partitions;
    private final Path dataDir;
    private final int maxBytes;

    public PartitionManager(Path dataDir, int maxBytes) {
        this.partitions = new ConcurrentHashMap<>();
        this.dataDir = dataDir;
        this.maxBytes = maxBytes;
        startupScan();
    }

    public void startupScan() {
        try {
            Files.createDirectories(dataDir);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir)) {
                for (Path p : stream) {
                    if (Files.isDirectory(p)) {
                        loadPartition(p);   
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadPartition(Path p) {
        String fileName = p.getFileName().toString();
        int index = fileName.lastIndexOf("-");
        if (index <= 0) {
            System.err.println("skipping non-partition directory: " + fileName);
            return;
        }
        try {
            String topic = fileName.substring(0, index);
            int position = Integer.parseInt(fileName.substring(index + 1));
            TopicPartition topicPartition = new TopicPartition(topic, position);
            this.partitions.put(topicPartition, new Partition(p, this.maxBytes));
        } catch (IOException | NumberFormatException e) {
            System.err.println("skipping " + fileName + ": " + e);
        }
    }

    // Lookup/read a partition
    public Partition lookUp(TopicPartition topicPartition) throws IOException {
        return partitions.computeIfAbsent(topicPartition, key -> {
            try {
                Path dir = dataDir.resolve(key.topic() + "-" + key.partition());
                return new Partition(dir, maxBytes);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public void close() {
        for (Partition p : partitions.values()) {
            p.close();
        }
    }
    
}
