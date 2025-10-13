package com.example.hm03;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class KafkaMessageService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final List<String> TOPICS = List.of("topic1", "topic2"); // Топики

    public boolean checkConnection() {
        try {
            kafkaTemplate.executeInTransaction(operations -> {
                operations.partitionsFor("topic1");
                return null;
            });
            System.out.println("Kafka доступна");
            return true;
        } catch (Exception e) {
            System.err.println("Kafka недоступна: " + e.getMessage());
            return false;
        }
    }

    @Transactional
    public void executeFirstTransaction() {
        System.out.println("Начало первой транзакции...");
        for (String topic : TOPICS) {
            for (int i = 1; i <= 5; i++) {
                String message = "Транзакция-1: Сообщение " + i + " в " + topic;
                kafkaTemplate.send(topic, "key-" + i, message);
                System.out.println("Отправлено: " + message);
            }
        }
        System.out.println("Первая транзакция завершена");
    }

    @Transactional
    public void executeSecondTransaction() {
        System.out.println("Начало второй транзакции...");
        for (String topic : TOPICS) {
            for (int i = 1; i <= 2; i++) {
                String message = "Транзакция-2: Сообщение " + i + " в " + topic;
                kafkaTemplate.send(topic, "key-" + i, message);
                System.out.println("Отправлено: " + message);
            }
        }

        System.out.println("Вторая транзакция отменена");
        throw new RuntimeException("Искусственная ошибка для отката транзакции");
    }

}
