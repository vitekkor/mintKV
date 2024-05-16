package com.mint.db.replication.model;

import com.mint.db.Raft;

public abstract class Message {
    public abstract String nodeUrl();

    public abstract Raft.AppendEntriesRequest request();
}
