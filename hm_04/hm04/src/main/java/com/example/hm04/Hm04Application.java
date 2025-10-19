package com.example.hm04;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class Hm04Application implements CommandLineRunner {

    @Autowired
    private KafkaStreamConfig streamConfig;

    public static void main(String[] args) {
        SpringApplication.run(Hm04Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        try {
            boolean isSuccessConnection = streamConfig.checkConnection();
            if (isSuccessConnection) {
                System.out.println("Kafka доступна");
            } else {
                System.out.println("Кафка недоступна");
            }
        } catch (Exception e) {
            System.out.println("Ошибка: " + e.getMessage());
        }
    }
}
