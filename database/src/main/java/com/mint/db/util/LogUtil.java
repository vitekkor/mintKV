package com.mint.db.util;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

public final class LogUtil {
    private static final JsonFormat.Printer protobufPrinter = JsonFormat.printer().includingDefaultValueFields();

    private LogUtil() {
    }

    public static String protobufMessageToString(Message message) {
        try {
            return message.getClass().getSimpleName() + protobufPrinter.print(message).replace('\n', ' ');
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
