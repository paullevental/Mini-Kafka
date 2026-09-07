package com.minikafka;

import java.io.IOException;
import java.nio.file.Path;

public class Broker {
    public static void main(String[] args) throws IOException {
        new Server(8080, Path.of("data/kafka.log")).startServer();
    }
}
