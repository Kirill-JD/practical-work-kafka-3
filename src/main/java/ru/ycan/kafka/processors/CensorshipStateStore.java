package ru.ycan.kafka.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Процессор для добавления заблокированных пользователей.
 */
@Slf4j
public class CensorshipStateStore implements Processor<String, Boolean, Void, Void> {
    public static final String CENSORSHIP_WORDS_STATE_STORE = "censorship_words_state_store";
    private ProcessorContext<Void, Void> context;
    private KeyValueStore<String, Boolean> censorshipWordsStore;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.context = context;
        // Получаем доступ к глобальному хранилищу
        this.censorshipWordsStore = context.getStateStore(CENSORSHIP_WORDS_STATE_STORE);
        log.info("Инициализирован процессор добавления заблокированных пользователей");
    }

    @Override
    public void process(Record<String, Boolean> record) {
        if (record.key() != null && record.value()) {
            censorshipWordsStore.put(record.key(), record.value());
        }
    }

    @Override
    public void close() {
        // Закрытие ресурсов (если необходимо)
    }
}
