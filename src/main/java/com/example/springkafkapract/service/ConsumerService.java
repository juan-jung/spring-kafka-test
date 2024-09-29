package com.example.springkafkapract.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ConsumerService {

    @KafkaListener(topics = "test", groupId = "spring")
    public void consumer(String message) {
        System.out.println(String.format("Subscribed : %s", message));
    }
}
