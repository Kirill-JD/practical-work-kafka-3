package ru.ycan.kafka.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Процессор для добавления запрещённых слов.
 */
@Slf4j
public class BlockedUsersStateStore implements Processor<String, String, Void, Void> {
    public static final String BLOCKED_USERS_STATE_STORE = "blocked_users_state_store";
    private ProcessorContext<Void, Void> context;
    private KeyValueStore<String, Boolean> blockedUsersStore;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.context = context;
        // Получаем доступ к глобальному хранилищу
        this.blockedUsersStore = context.getStateStore(BLOCKED_USERS_STATE_STORE);
        log.info("Инициализирован процессор добавления заблокированных пользователей");
    }

    @Override
    public void process(Record<String, String> record) {
        if (record.key() == null || record.value() == null) {
            return;
        }
        var key = getKey(record.key(), record.value());
        if (blockedUsersStore.get(key) == null) {
            blockedUsersStore.put(key, true);
            log.info("Пользователь '{}' заблокировал пользователя '{}'", record.key(), record.value());
        } else {
            log.info("Пользователь '{}' уже ранее заблокировал пользователя '{}'", record.key(), record.value());
        }
    }

    @Override
    public void close() {
        // Закрытие ресурсов (если необходимо)
    }

    public static String getKey(String userIdTo, String userIdFrom) {
        return String.join(":", userIdTo, userIdFrom);
    }
}
