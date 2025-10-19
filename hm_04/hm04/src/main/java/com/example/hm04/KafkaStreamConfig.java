package com.example.hm04;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;


@Configuration
public class KafkaStreamConfig {

    @Autowired
    private KafkaConfigHolder kafkaConfigHolder;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Bean
    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {

        // Читаем входные события
        KStream<String, String> events = streamsBuilder.stream(
                kafkaConfigHolder.getTopic(),
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // Группируем по ключу
        KGroupedStream<String, String> grouped = events.groupByKey();

        // Сессия с таймаутом inactivity gap из настроек
        KTable<Windowed<String>, Long> sessionCounts = grouped
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(
                        Duration.ofMinutes(kafkaConfigHolder.getListeningMinutes())))
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        // Преобразуем в стрим и отправляем в топик
        sessionCounts.toStream()
                .map((windowedKey, count) -> {
                    long safeCount = Optional.ofNullable(count).orElse(0L);

                    String key = windowedKey.key();
                    String value = safeCount + " (window: "
                            + windowedKey.window().startTime() + " - "
                            + windowedKey.window().endTime() + ")";

                    // Логируем в консоль
                    System.out.println("Результат сессии -> ключ: " + key + ", значение: " + value);

                    return KeyValue.pair(key, Long.toString(safeCount));
                })
                .to(kafkaConfigHolder.getEventCounts(),
                        Produced.with(Serdes.String(), Serdes.String()));

        return events;
    }

    @KafkaListener(topics = "#{kafkaConfigHolder.eventCounts}", groupId = "#{kafkaConfigHolder.groupId}")
    public void listen(String message) {
        System.out.println("Получено из event-counts: " + message);
    }

    public boolean checkConnection() {
        try {
            kafkaTemplate.execute(operations -> {
                operations.partitionsFor(kafkaConfigHolder.getTopic());
                return null;
            });
            System.out.println("Kafka доступна");
            return true;
        } catch (Exception e) {
            System.err.println("Kafka недоступна: " + e.getMessage());
            return false;
        }
    }

    @Bean
    public Properties kafkaStreamsProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaConfigHolder.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigHolder.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }
}
