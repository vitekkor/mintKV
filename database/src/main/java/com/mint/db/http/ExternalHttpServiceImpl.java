package com.mint.db.http;

import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mint.DatabaseServiceOuterClass;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class ExternalHttpServiceImpl {

    private static final int PORT = 8080;
    private static final ExternalGrpcClient grpcClient = new ExternalGrpcClient("localhost:50051");
    private static final Gson gson = new Gson();

    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/insert", new InsertHandler());
        server.createContext("/delete", new DeleteHandler());
        server.createContext("/get", new GetHandler());
        server.setExecutor(Executors.newFixedThreadPool(10));
        server.start();
        System.out.println("Server started on port " + PORT);
    }

    static class InsertHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("PUT".equals(exchange.getRequestMethod())) {
                String body = new String(exchange.getRequestBody().readAllBytes());
                JsonObject json = gson.fromJson(body, JsonObject.class);
                String key = json.get("key").getAsString();
                String value = json.get("value").getAsString();
                boolean uncommitted = json.get("uncommitted").getAsBoolean();

                DatabaseServiceOuterClass.InsertRequest request = DatabaseServiceOuterClass.InsertRequest.newBuilder()
                        .setKey(key)
                        .setValue(value)
                        .setUncommitted(uncommitted)
                        .build();

                grpcClient.insert(request, response -> {
                    try {
                        String responseBody = gson.toJson(response);
                        exchange.sendResponseHeaders(200, responseBody.length());
                        OutputStream os = exchange.getResponseBody();
                        os.write(responseBody.getBytes());
                        os.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }

    static class DeleteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("DELETE".equals(exchange.getRequestMethod())) {
                String body = new String(exchange.getRequestBody().readAllBytes());
                JsonObject json = gson.fromJson(body, JsonObject.class);
                String key = json.get("key").getAsString();
                boolean uncommitted = json.get("uncommitted").getAsBoolean();

                DatabaseServiceOuterClass.DeleteRequest request = DatabaseServiceOuterClass.DeleteRequest.newBuilder()
                        .setKey(key)
                        .setUncommitted(uncommitted)
                        .build();

                grpcClient.delete(request, response -> {
                    try {
                        String responseBody = gson.toJson(response);
                        exchange.sendResponseHeaders(200, responseBody.length());
                        OutputStream os = exchange.getResponseBody();
                        os.write(responseBody.getBytes());
                        os.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }

    static class GetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                String key = exchange.getRequestURI().getQuery().split("=")[1];

                DatabaseServiceOuterClass.GetRequest request = DatabaseServiceOuterClass.GetRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8(key))
                        .setMode(DatabaseServiceOuterClass.ReadMode.READ_COMMITTED)
                        .build();

                grpcClient.get(request, response -> {
                    try {
                        String responseBody = gson.toJson(response);
                        exchange.sendResponseHeaders(200, responseBody.length());
                        OutputStream os = exchange.getResponseBody();
                        os.write(responseBody.getBytes());
                        os.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }
}
