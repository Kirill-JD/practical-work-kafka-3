package ru.ycan.kafka.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import ru.ycan.pojo.Message;
import ru.ycan.utils.bson.BsonMapper;

import static ru.ycan.kafka.processors.BlockedUsersStateStore.BLOCKED_USERS_STATE_STORE;
import static ru.ycan.kafka.processors.BlockedUsersStateStore.getKey;

/**
 * Процессор для фильтрации сообщений от заблокированных пользователей.
 */
@Slf4j
public class FilterBlockedUsersProcessor implements Processor<String, byte[], String, byte[]> {
    private ProcessorContext<String, byte[]> context;
    private KeyValueStore<String, Boolean> blockedUsersStore;

    @Override
    public void init(ProcessorContext<String, byte[]> context) {
        this.context = context;
        this.blockedUsersStore = context.getStateStore(BLOCKED_USERS_STATE_STORE);
        log.info("Инициализирован процессор фильтрации сообщений от заблокированных пользователей");
    }

    @Override
    public void process(Record<String, byte[]> record) {
        try {
            var message = BsonMapper.readValue(record.value(), Message.class);
            var key = getKey(message.userIdTo(), message.userIdFrom());
            if (blockedUsersStore.get(key) == null) {
                context.forward(record);
            } else {
                log.warn("Пришло сообщение для '{}' от заблокированного пользователя '{}'",
                         message.userIdTo(), message.userIdFrom());
            }
        } catch (Exception e) {
            log.error("Не удалось прочитать сообщение", e);
        }
    }

    @Override
    public void close() {
        // Закрытие ресурсов (если необходимо)
    }
}
