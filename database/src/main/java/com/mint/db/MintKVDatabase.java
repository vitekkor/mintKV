package com.mint.db;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mint.db.config.InjectionModule;
import com.mint.db.grpc.server.Server;
import com.mint.db.http.server.ExternalHttpServer;

public class MintKVDatabase {

    public static void main(String[] args) throws InterruptedException {
        Injector injector = Guice.createInjector(new InjectionModule());
        ExternalHttpServer httpServer = injector.getInstance(ExternalHttpServer.class);
        httpServer.start();
        Server grpcServer = injector.getInstance(Server.class);
        grpcServer.start();
        grpcServer.blockUntilShutdown();
    }
}
