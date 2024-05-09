package com.mint.db.replication.model.impl;

public enum OperationType {
    GET(1), PUT(2), DELETE(3);

    private final int value;

    OperationType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static OperationType fromLong(long operationType) {
        return switch ((int) operationType) {
            case 1 -> GET;
            case 2 -> PUT;
            case 3 -> DELETE;
            default -> throw new IllegalArgumentException("Invalid OperationType value: " + operationType);
        };
    }
}
