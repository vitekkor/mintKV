package com.mint.db.replication.model.impl;

import com.mint.db.Raft;
import com.mint.db.replication.model.Message;

public class LeaderMessage extends Message {
    private final String leaderUrl;
    private final Raft.AppendEntriesRequest request;

    public LeaderMessage(String leaderUrl, Raft.AppendEntriesRequest request) {
        this.leaderUrl = leaderUrl;
        this.request = request;
    }

    @Override
    public String nodeUrl() {
        return leaderUrl;
    }

    @Override
    public Raft.AppendEntriesRequest request() {
        return request;
    }
}
