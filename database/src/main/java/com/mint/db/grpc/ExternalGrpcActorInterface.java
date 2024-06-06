package com.mint.db.grpc;

import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;
import io.grpc.stub.StreamObserver;

public interface ExternalGrpcActorInterface {
    /**
     * Sends the {@code commandResult} to the client.
     *
     * @param commandResult result of applying client command
     */
    void onClientCommandResult(
            Command command,
            CommandResult commandResult
    );

    void addClientCommandCallback(Command command, StreamObserver<?> responseObserver);
}
