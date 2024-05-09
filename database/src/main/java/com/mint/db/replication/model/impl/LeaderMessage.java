package com.mint.db.replication.model.impl;

import com.mint.db.Raft;
import com.mint.db.replication.model.Message;

public class LeaderMessage extends Message {
    private final String LeaderUrl;
    private final Raft.AppendEntriesRequest request;

    public LeaderMessage(String leaderUrl, Raft.AppendEntriesRequest request) {
        this.LeaderUrl = leaderUrl;
        this.request = request;
    }
}
