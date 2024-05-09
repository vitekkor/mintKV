package com.mint.db.replication.model.impl;

public enum OperationType {
    GET(0), PUT(1), DELETE(2);

    private final int value;

    OperationType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static OperationType fromLong(long operationType) {
        return switch ((int) operationType) {
            case 0 -> GET;
            case 1 -> PUT;
            case 2 -> DELETE;
            default -> throw new IllegalArgumentException("Invalid OperationType value: " + operationType);
        };
    }
}
