package com.mint.db;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mint.db.config.InjectionModule;
import com.mint.db.grpc.server.Server;

public class MintKVDatabase {

    public static void main(String[] args) throws InterruptedException {
        Injector injector = Guice.createInjector(new InjectionModule());
        Server server = injector.getInstance(Server.class);
        server.start();
        server.blockUntilShutdown();
    }
}
