package ru.ycan.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BooleanSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import ru.ycan.kafka.processors.BlockedUsersStateStore;
import ru.ycan.kafka.processors.CensorshipProcessor;
import ru.ycan.kafka.processors.CensorshipStateStore;
import ru.ycan.kafka.processors.FilterBlockedUsersProcessor;
import ru.ycan.pojo.Message;
import ru.ycan.utils.bson.BsonMapper;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static ru.ycan.kafka.processors.BlockedUsersStateStore.BLOCKED_USERS_STATE_STORE;
import static ru.ycan.kafka.processors.CensorshipStateStore.CENSORSHIP_WORDS_STATE_STORE;

@Slf4j
public class KafkaProcess {
    private static final String BOOTSTRAP_SERVERS = "kafka1:29092";
    //region топики для работы
    private static final String MESSAGES_TOPIC = "messages";
    private static final String FILTERED_MESSAGES_TOPIC = "filtered_messages";
    private static final String BLOCKED_USERS_TOPIC = "blocked_users";
    // добавлен вне постановки, для возможности динамического обновления списка слов
    private static final String CENSORSHIP_WORDS_TOPIC = "censorship_words";
    //endregion

    /**
     * Точка входа.
     */
    public void start() {
        try {
            // Создаем топики
            createTopics();

            // Создаем и запускаем Kafka Streams приложение
            KafkaStreams streams = buildStreamsApplication();
            final CountDownLatch latch = new CountDownLatch(1);

            // Обработка завершения работы
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Завершение работы приложения...");
                streams.close();
                latch.countDown();
            }));

            // Запускаем Kafka Streams
            streams.start();
            log.info("Приложение Sales Processor запущено");

            // Отправляем тестовые данные о продажах
            loadTestData();

            // Даем время на обработку данных
            log.info("Ожидание обработки данных...");
            TimeUnit.SECONDS.sleep(5);

            // Ожидаем сигнала завершения
            latch.await();

        } catch (Exception e) {
            log.error("Ошибка при запуске приложения: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    /**
     * Создает и настраивает приложение Kafka Streams с использованием Processor API
     */
    private static KafkaStreams buildStreamsApplication() {
        // Настройка свойств Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "filter-messages-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Отключаем кеширование для наглядности
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        // Создаем топологию с использованием Processor API
        Topology topology = new Topology();

        topology.addSource(
                "MessagesSource",
                Serdes.String().deserializer(),
                Serdes.ByteArray().deserializer(),
                MESSAGES_TOPIC
        );

        // Создаем билдер для глобального хранилища заблокированных пользователей
        StoreBuilder<KeyValueStore<String, Boolean>> blockedUsersStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(BLOCKED_USERS_STATE_STORE),
                Serdes.String(),
                Serdes.Boolean()
        ).withLoggingDisabled(); // Отключаем логирование для глобального хранилища

        // Добавляем хранилище к топологии
        topology.addGlobalStore(
                blockedUsersStoreBuilder,           // Билдер хранилища
                BLOCKED_USERS_STATE_STORE,          // Имя для источника глобального хранилища
                Serdes.String().deserializer(),     // Десериализатор ключа
                Serdes.String().deserializer(),     // Десериализатор значения
                BLOCKED_USERS_TOPIC,                // Входной топик для хранилища
                "BlockedUsersProcessor",            // Имя процессора
                BlockedUsersStateStore::new         // Поставщик процессора
        );

        // Создаем билдер для глобального хранилища запрещённых слов
        StoreBuilder<KeyValueStore<String, Boolean>> censorshipWordsStoreBuilder  = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(CENSORSHIP_WORDS_STATE_STORE),
                Serdes.String(),
                Serdes.Boolean()
        ).withLoggingDisabled(); // Отключаем логирование для глобального хранилища

        topology.addGlobalStore(
                censorshipWordsStoreBuilder,
                CENSORSHIP_WORDS_STATE_STORE,
                Serdes.String().deserializer(),
                Serdes.Boolean().deserializer(),
                CENSORSHIP_WORDS_TOPIC,
                "AddCensorshipWordProcessor",
                CensorshipStateStore::new
        );

        topology.addProcessor(
                "FilterBlockedUsersProcessor",
                FilterBlockedUsersProcessor::new,
                "MessagesSource"
        );

        topology.addProcessor(
                "CensorshipProcessor",
                CensorshipProcessor::new,
                "FilterBlockedUsersProcessor"
        );

        topology.addSink("Sink",
                         FILTERED_MESSAGES_TOPIC,
                         Serdes.String().serializer(),
                         Serdes.ByteArray().serializer(),
                         "CensorshipProcessor");

        System.out.println("Топология: " + topology.describe());

        // Создаем приложение Kafka Streams
        return new KafkaStreams(topology, props);
    }

    /**
     * Инициализация топиков.
     */
    private static void createTopics() {
        log.info("Создаём топики");
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            NewTopic messagesTopic = new NewTopic(MESSAGES_TOPIC, 1, (short) 1);
            NewTopic filteredMessagesTopic = new NewTopic(FILTERED_MESSAGES_TOPIC, 1, (short) 1);
            NewTopic blockedUsersTopic = new NewTopic(BLOCKED_USERS_TOPIC, 1, (short) 1);
            NewTopic censorshipWordsTopic = new NewTopic(CENSORSHIP_WORDS_TOPIC, 1, (short) 1);

            var topics = List.of(messagesTopic, filteredMessagesTopic, blockedUsersTopic, censorshipWordsTopic);
            adminClient.createTopics(topics).all().get();
            log.info("Созданы топики: {}", topics);
        } catch (Exception e) {
            log.error("Ошибка при создании топиков (возможно, они уже существуют): {}", e.getMessage());
        }
    }

    /**
     * Создание тестовых данных и сообщений для топиков.
     */
    private static void loadTestData() {
        var userIds = List.of(buildUserId("Alex"),
                              buildUserId("Buster"),
                              buildUserId("Carl"));

        loadTestCensorshipWords();
        loadTestBlockedUsers(userIds);
        loadTestMessages(userIds);
    }

    /**
     * Тестовые запрещённые слова для последующего цензурирования.
     */
    private static void loadTestCensorshipWords() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BooleanSerializer.class);

        var censorshipWords = List.of("пупа", "лупа");
        try (KafkaProducer<String, Boolean> producer = new KafkaProducer<>(producerProps)) {
            censorshipWords.forEach(
                    word -> producer.send(new ProducerRecord<>(CENSORSHIP_WORDS_TOPIC, word, true)));
            producer.flush();
        } catch (Exception e) {
            log.error("Не удалось отправить данные в топик '{}'", CENSORSHIP_WORDS_TOPIC, e);
        }
    }

    /**
     * Тестовые данные о заблокированных пользователях.
     *
     * @param userIds uuid пользователей (внутри метода рандом)
     */
    private static void loadTestBlockedUsers(List<String> userIds) {
        Random random = new Random();
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (var userId : userIds) {
                var blockedUserId = userIds.stream()
                                           .filter(u -> !Objects.equals(u, userId))
                                           .toList()
                                           .get(random.nextInt(userIds.size() - 1));
                producer.send(new ProducerRecord<>(BLOCKED_USERS_TOPIC, userId, blockedUserId));
                log.info("Отправлено сообщение от '{}' о блокировке пользователя '{}'", userId, blockedUserId);
            }
            producer.flush();
        } catch (Exception e) {
            log.error("Не удалось отправить данные в топик '{}'", BLOCKED_USERS_TOPIC, e);
        }
    }

    /**
     * Тестовые сообщения (от кого, кому, текст сообщения).
     *
     * @param userIds uuid пользователей (внутри метода рандом)
     */
    private static void loadTestMessages(List<String> userIds) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        Random random = new Random();
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
            var testMessages = List.of(
                    "Погода хорошая, но настроение немного странное.",
                    "Я купил новый телефон и он супер удобный.",
                    "На вечеринке кто-то постоянно говорил пупа, это смешно.",
                    "Завтра пойду в парк гулять с друзьями.",
                    "У моей бабушки есть лупа для чтения.",
                    "Я сделал кофе и начал читать книгу.",
                    "Дети играли на улице, кричали пупа и смеялись.",
                    "Вчера смотрел фильм, очень понравился сюжет и музыка.",
                    "В магазине увидел интересные вещи, но понравилась только лупа.",
                    "Прислали новое задание, буду его колупать после прогулки"
            );

            for (var testMessage : testMessages) {
                var userIdFrom = userIds.get(random.nextInt(userIds.size()));
                var userIdTo = userIds.stream()
                                      .filter(u -> !Objects.equals(u, userIdFrom))
                                      .toList()
                                      .get(random.nextInt(userIds.size() - 1));
                var message = Message.builder()
                                     .userIdFrom(userIdFrom)
                                     .userIdTo(userIdTo)
                                     .messageText(testMessage)
                                     .build();
                var value = BsonMapper.writeValueAsBytes(message);
                producer.send(new ProducerRecord<>(MESSAGES_TOPIC, UUID.randomUUID().toString(), value));
                log.info("Отправлено тестовое сообщение '{}'", testMessage);
            }

            producer.flush();
            log.info("Все тестовые сообщения отправлены");
        } catch (Exception e) {
            log.error("Не удалось отправить данные в топик '{}'", MESSAGES_TOPIC, e);
        }
    }

    private static String buildUserId(String prefix) {
        // чисто для более удобной навигации по логам
        return String.join("_", prefix, UUID.randomUUID().toString());
    }
}
