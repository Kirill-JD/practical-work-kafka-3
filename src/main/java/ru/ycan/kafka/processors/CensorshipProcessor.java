package ru.ycan.kafka.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import ru.ycan.pojo.Message;
import ru.ycan.utils.bson.BsonMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static ru.ycan.kafka.processors.CensorshipStateStore.CENSORSHIP_WORDS_STATE_STORE;

/**
 * Процессор для цензурирования сообщений.
 */
@Slf4j
public class CensorshipProcessor implements Processor<String, byte[], String, byte[]> {
    private ReadOnlyKeyValueStore<String, Boolean> censorshipWordsStore;
    private ProcessorContext<String, byte[]> context;
    private volatile Pattern cachedPattern;

    @Override
    public void init(ProcessorContext<String, byte[]> context) {
        this.context = context;
        this.censorshipWordsStore = context.getStateStore(CENSORSHIP_WORDS_STATE_STORE);
        log.info("Инициализирован процессор цензурирования сообщений");

        // обновляем Pattern каждые 5 секунд
        context.schedule(
                Duration.ofSeconds(5),
                PunctuationType.WALL_CLOCK_TIME,
                timestamp -> rebuildPattern()
        );
    }

    @Override
    public void process(Record<String, byte[]> record) {
        try {
            if (cachedPattern == null) {
                rebuildPattern();
            }
            var message = BsonMapper.readValue(record.value(), Message.class);

            var messageTextWithCensorship = cachedPattern.matcher(message.messageText())
                                                         .replaceAll(m -> "*".repeat(m.group().length()));
            context.forward(record.withValue(
                    BsonMapper.writeValueAsBytes(message.withMessageText(messageTextWithCensorship))));
        } catch (Exception e) {
            log.error("Не удалось проверить сообщение на запрещённые слова", e);
        }
    }

    @Override
    public void close() {
        // Закрытие ресурсов (если необходимо)
    }

    private void rebuildPattern() {
        List<String> censorshipWords = new ArrayList<>();
        try (var censorshipWordsIterator = censorshipWordsStore.all()) {
            censorshipWordsIterator.forEachRemaining(keyValue -> {
                if (keyValue.value) {
                    censorshipWords.add(keyValue.key);
                }
            });
            if (censorshipWords.isEmpty()) {
                cachedPattern = Pattern.compile("$^");
                return;
            }
            var patternString = censorshipWords.stream()
                                               .map(Pattern::quote)
                                               .reduce((w1, w2) -> String.join("|", w1, w2))
                                               .orElse("");
            this.cachedPattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE);
            log.info("Обновлён список запрещённых слов: {}", censorshipWords.size());
        }
    }
}
