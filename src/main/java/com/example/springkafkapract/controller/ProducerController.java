package com.example.springkafkapract.controller;

import com.example.springkafkapract.service.ProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class ProducerController {

    private final ProducerService producerService;

    @PostMapping("/message")
    public void publishMessage(@RequestParam String msg) {
        producerService.pub(msg);
    }
}
