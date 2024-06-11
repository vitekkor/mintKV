package com.mint.db;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mint.db.config.InjectionModule;
import com.mint.db.grpc.server.Server;
import com.mint.db.http.server.ExternalHttpServer;

import java.util.Arrays;

public class MintKVDatabase {

    public static void main(String[] args) throws InterruptedException {

        boolean isHttpMode = Arrays.stream(args).anyMatch(arg -> arg.equals("--http"));

        if (isHttpMode) {
            Injector injector = Guice.createInjector(new InjectionModule());
            ExternalHttpServer server = injector.getInstance(ExternalHttpServer.class);
            server.start();
        } else {
            Injector injector = Guice.createInjector(new InjectionModule());
            Server server = injector.getInstance(Server.class);
            server.start();
            server.blockUntilShutdown();
        }
    }
}
