package com.example.springkafkapract.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    String topicName = "test";

    public void pub(String msg) {
        kafkaTemplate.send(topicName, msg);
    }
}
