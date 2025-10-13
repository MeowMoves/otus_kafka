package com.example.hm03;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Hm03Application implements CommandLineRunner {

    @Autowired
    private KafkaMessageService kafkaMessageService;

    @Autowired
    private KafkaMessageReader kafkaMessageReader;

    public static void main(String[] args) {
        SpringApplication.run(Hm03Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        try {
            boolean isSuccessConnection = kafkaMessageService.checkConnection(); // Флаг успешного подключения

            if (isSuccessConnection) {
                System.out.println("Начало выполнения транзакций");
                // Первая транзакция - отправка 5 сообщений в каждый топик и подтверждение
                kafkaMessageService.executeFirstTransaction();

                // Ожидаем выброс исключения
                // Вторая транзакция - отправка 2 сообщений в каждый топик и отмена
                try {
                    kafkaMessageService.executeSecondTransaction();
                } catch (Error e) {
                    System.err.println("Ожидаемое исключение после второй транзакции: " + e.getMessage());
                }

                Thread.sleep(5000); // Время для обработки

                System.out.println("Чтение сообщений");
                kafkaMessageReader.readMessagesFromTopics();

                System.out.println("Конец выполнения транзакций");
            } else {
                System.err.println("Не удалось подключиться к Kafka");
            }

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

}
