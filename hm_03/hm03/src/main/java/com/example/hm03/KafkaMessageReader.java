package com.example.hm03;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Component
public class KafkaMessageReader {

    private final List<String> receivedMessages = new ArrayList<>();
    private final CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(topics = {"topic1", "topic2"}, groupId = "consumer-group")
    public void listen(String message) {
        receivedMessages.add(message);
        System.out.println("Получено: " + message);

        // Если получили достаточно сообщений, завершаем ожидание
        if (receivedMessages.size() >= 10) {
            latch.countDown();
        }
    }

    public void readMessagesFromTopics() {
        try {
            System.out.println("Ожидание сообщений из топиков...");

            // Ждем не более 5 секунд для получения сообщений
            boolean received = latch.await(5, TimeUnit.SECONDS);

            if (received) {
                System.out.println("Всего получено сообщений: " + receivedMessages.size());
                System.out.println("Подтверждённые сообщения:");

                receivedMessages.stream()
                        .filter(msg -> msg.contains("Транзакция-1"))
                        .forEach(System.out::println);

            } else {
                System.out.println("Таймаут ожидания сообщений. Получено: " + receivedMessages.size());
                if (!receivedMessages.isEmpty()) {
                    System.out.println("Полученные сообщения:");
                    receivedMessages.forEach(System.out::println);
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Ошибка при чтении сообщений: " + e.getMessage());
        }
    }

}
