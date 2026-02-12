package io.kestra.plugin.azure.servicebus;

import com.azure.core.util.BinaryData;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;
import java.util.List;

import static io.kestra.core.utils.Rethrow.throwFunction;

public enum SerdeType {
    STRING,
    JSON;

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson(false);

    public List<Object> deserialize(List<BinaryData> message) throws IOException {
        return message.stream().map(
            throwFunction(this::deserialize)
        ).toList();
    }



    public Object deserialize(BinaryData message) {
        return switch (this) {
            case STRING -> message.toString();
            case JSON -> {
                try {
                    yield OBJECT_MAPPER.readValue(message.toBytes(), Object.class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }


    public BinaryData serialize(Object message) throws IOException {
        return BinaryData.fromString(
            switch (this) {
                case STRING -> message.toString();
                case JSON -> OBJECT_MAPPER.writeValueAsString(message);
            }
        );
    }
}
