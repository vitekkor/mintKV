package com.mint.db;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mint.db.config.ConfigParser;
import com.mint.db.config.InjectionModule;
import com.mint.db.config.NodeConfig;
import com.mint.db.grpc.server.Server;

import java.io.FileNotFoundException;

public class MintKVDatabase {

    public static void main(String[] args) throws InterruptedException {
        Injector injector = Guice.createInjector(new InjectionModule());
        Server server = injector.getInstance(Server.class);
        server.start();
        server.blockUntilShutdown();
    }
}
