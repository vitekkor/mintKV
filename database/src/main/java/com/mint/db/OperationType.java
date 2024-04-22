package com.mint.db;

public enum OperationType {
    GET(0), PUT(1), DELETE(2);

    private final int value;

    OperationType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
