package com.minikafka;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// TCP listener. One virtual thread per connection, one length-prefixed frame per read.
public class Server {

    private final ServerSocket serverSocket;
    private final Log log;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    public Server(int port, Path logfile) throws IOException{
        this.serverSocket = new ServerSocket(port);
        this.log = new Log(logfile);
    }

    public void startServer() throws IOException{
        while (true) {
            Socket clientSocket = serverSocket.accept();
            executorService.execute(() -> handleClient(clientSocket));
        }
    }

    public void handleClient(Socket clientSocket) {
        try (clientSocket ){
            // bytes arriving from client over TCP connection 
            DataInputStream inputStream = new DataInputStream(clientSocket.getInputStream());
            int n = inputStream.readInt();
            if (n < 8 || n > 1_000_000) {
                throw new IOException("error, frame lengt: " + n);
            }
            byte[] frame = new byte[n];
            inputStream.readFully(frame);
            ByteBuffer buffer = ByteBuffer.wrap(frame); 
            short apiKey = buffer.getShort();
            short apiVersion = buffer.getShort();
            int correlationId = buffer.getInt();
            RequestHeader header = new RequestHeader(apiKey, apiVersion, correlationId);
            byte[] payload = new byte[buffer.remaining()];
            buffer.get(payload);
            System.out.println("Request Header: " + header);
            handleRequest(payload, apiKey, clientSocket);
        
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void handleRequest(byte[] payload, short apiKey, Socket clientSocket) throws IOException{
        switch (apiKey) {
            case 0 -> log.append(payload);
            case 1 -> 
            {
                byte[] data = log.readAll();
                var outputStream = new DataOutputStream(clientSocket.getOutputStream());
                outputStream.writeInt(data.length);
                outputStream.write(data);
                outputStream.flush(); 
            }

            default -> System.err.println("unknown api key: " + apiKey);
        }
    }
}
