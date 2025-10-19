package com.example.hm04;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Getter
public class KafkaConfigHolder {

    @Value("${hm04.topic-value}")
    private String topic;

    @Value("${hm04.listening-minutes}")
    private int listeningMinutes;

    @Value("${spring.kafka.streams.application-id}")
    private String applicationId;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${hm04.output-topic-value}")
    private String eventCounts;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
}
