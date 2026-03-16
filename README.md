# Практическая работа 3

По коду есть комментарии с описанием тех или иных методов. По структуре (основное):
- `Main` - запуск
- `Handler` - прослойка (можно сказать задел на будущее, не более)
- `BsonMapper` - вспомогательная утилита для сериализации/десериализации
- `Message` - pojo с сообщением от user к user
- `KafkaProcess` - основной класс с созданием топологии и наполнением тестовыми данными
- `BlockedUsersStateStore` - наполнение хранилища заблокированных пользователей
- `FilterBlockedUsersProcessor` - процессор с фильтрацией сообщения от заблокированных пользователей
- `CensorshipStateStore` - наполнение хранилища запрещёнными словами для цензуры
- `CensorshipProcessor` - процессор с логикой цензурирования

### При запуске локально
- Поправить поле `BOOTSTRAP_SERVERS` на `private static final String BOOTSTRAP_SERVERS = "localhost:9092";`
- Запустить main

### При запуске через docker-compose
- Запустить `docker-compose.yaml` из корня проекта

### Тестовые данные
#### Данные которые прибиты в коде (можно корректировать перед запуском)

На данный момент есть несколько методов в классе `KafkaProcess` которые отвечают за наполнение тестовыми данными
- метод `loadTestBlockedUsers` для наполнения глобального хранилища (внутри метода рандом) заблокированными пользователями (используется <Имя>_UUID кто заблокировал и кого)
```text
// чисто пример, ибо рандом
"Alex" -> "Buster",
"Buster" -> "Carl",
"Carl" -> "Alex"
```

- метод `loadTestCensorshipWords` для наполнения глобального хранилища запрещённых слов
```text
"пупа", 
"лупа"
```

- метод `loadTestMessages` для наполнения топика `messages` тестовыми сообщениями (ниже только текст сообщений, есть внутренняя логика с рандомным заполнением отправителя/получателя сообщения)
```text
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
```

#### Можно добавить свои данные в топики в runtime, чтобы проверить работу на своих примерах
- для отправки данных о заблокированных пользователях в топик `blocked_users`
```java
try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
    producer.send(new ProducerRecord<>("blocked_users", '<userId>', '<blockedUserId>'));
    producer.flush();
}
```

- для отправки данных для наполнения глобального хранилища запрещённых слов через топик `censorship_words`
```java
try (KafkaProducer<String, Boolean> producer = new KafkaProducer<>(producerProps)) {
    producer.send(new ProducerRecord<>("censorship_words", '<word>', true));
    producer.flush();
}
```

- для отправки тестовых сообщений в топик `messages`
```java
try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps))) {
    var message = Message.builder()
                         .userIdFrom('<userId>')
                         .userIdTo('<userId>')
                         .messageText('<testMessage>')
                         .build();
    var value = BsonMapper.writeValueAsBytes(message);
    producer.send(new ProducerRecord<>("messages", UUID.randomUUID().toString(), value));
    producer.flush();
}
```