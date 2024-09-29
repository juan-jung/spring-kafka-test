package com.example.springkafkapract.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class StreamService {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    public void buildPipeline(StreamsBuilder sb) {

        KStream<String, String> stream =  sb.stream("test", Consumed.with(STRING_SERDE, STRING_SERDE));
        //test topic에 aaa를 포함하는 msg가 들어오묜 TargetTopic으로 필터링
        stream.filter((key, value) -> value.contains("aaa")).to("TargetTopic");
    }
}
