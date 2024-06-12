package com.mint.db.http.server.dto;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public record InsertRequestDto(String key, String value, boolean uncommitted) {
    public static InsertRequestDto valueOf(HttpExchange exchange) {
        try {
            InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), "utf-8");

            BufferedReader br = new BufferedReader(isr);

            String query = br.readLine();
            Gson gson = new Gson();
            return gson.fromJson(query, InsertRequestDto.class);
        } catch (IOException e) {
            System.out.println("Failed to parse request body");
            return null;
        } finally {
            try {
                exchange.getRequestBody().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
