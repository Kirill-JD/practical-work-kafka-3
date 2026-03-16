package ru.ycan.utils.bson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;
import lombok.experimental.UtilityClass;

import java.io.IOException;

/**
 * Вспомогательный класс для сериализации/десериализации сообщений в формате bson
 */
@UtilityClass
public class BsonMapper {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new BsonFactory());

    public static <T> byte[] writeValueAsBytes(T value) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsBytes(value);
    }

    public static <T> T readValue(byte[] value, Class<T> clazz) throws IOException {
        return OBJECT_MAPPER.readValue(value, clazz);
    }
}
