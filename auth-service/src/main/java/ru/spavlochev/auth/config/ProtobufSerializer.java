package ru.spavlochev.auth.config;

import com.google.protobuf.MessageLite;
import org.apache.kafka.common.serialization.Serializer;

public class ProtobufSerializer<T extends MessageLite> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        return data.toByteArray();
    }
}
