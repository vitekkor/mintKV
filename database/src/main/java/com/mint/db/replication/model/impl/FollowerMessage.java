package com.mint.db.replication.model.impl;

import com.mint.db.Raft;
import com.mint.db.replication.model.Message;

public class FollowerMessage extends Message {
    private final String followerUrl;
    private final Raft.AppendEntriesRequest request;

    public FollowerMessage(String followerUrl, Raft.AppendEntriesRequest request) {
        this.followerUrl = followerUrl;
        this.request = request;
    }
}
