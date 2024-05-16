package com.mint.db.grpc.server;

import com.mint.DatabaseServiceGrpc;
import com.mint.DatabaseServiceOuterClass;
import io.grpc.stub.StreamObserver;

public class ExternalServiceImpl extends DatabaseServiceGrpc.DatabaseServiceImplBase {
    @Override
    public void insert(
            DatabaseServiceOuterClass.InsertRequest request,
            StreamObserver<DatabaseServiceOuterClass.SuccessfulWriteResponse> responseObserver
    ) {
        super.insert(request, responseObserver);
    }

    @Override
    public void delete(
            DatabaseServiceOuterClass.DeleteRequest request,
            StreamObserver<DatabaseServiceOuterClass.SuccessfulWriteResponse> responseObserver
    ) {
        super.delete(request, responseObserver);
    }

    @Override
    public void get(
            DatabaseServiceOuterClass.GetRequest request,
            StreamObserver<DatabaseServiceOuterClass.GetResponse> responseObserver
    ) {
        super.get(request, responseObserver);
    }
}
