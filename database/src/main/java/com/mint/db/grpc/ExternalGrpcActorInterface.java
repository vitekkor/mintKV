package com.mint.db.grpc;

import com.mint.db.raft.RaftActor;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;

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

    void setRaftActor(
            RaftActor raftActor
    );
}
