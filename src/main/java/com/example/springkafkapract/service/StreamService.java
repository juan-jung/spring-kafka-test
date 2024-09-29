package com.example.springkafkapract.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class StreamService {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    public void buildPipeline(StreamsBuilder sb) {

//        //test topic에 aaa를 포함하는 msg가 들어오묜 TargetTopic으로 필터링
//        KStream<String, String> stream =  sb.stream("test", Consumed.with(STRING_SERDE, STRING_SERDE));
//        stream.filter((key, value) -> value.contains("aaa")).to("TargetTopic");


        //join : key를 기준으로 ex) 1:leftvalue
        KStream<String, String> left =  sb.stream(
                "leftTopic",
                Consumed.with(STRING_SERDE, STRING_SERDE)).selectKey((k,v)->v.substring(0,v.indexOf(":")));

        KStream<String, String> right =  sb.stream(
                "rightTopic",
                Consumed.with(STRING_SERDE, STRING_SERDE)).selectKey((k,v)->v.substring(0,v.indexOf(":")));

        left.print(Printed.toSysOut());
        right.print(Printed.toSysOut());

        KStream<String, String> joinStream = left.join(
                right,
                (leftValue, rightValue) -> leftValue + " : " + rightValue,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1)));

        joinStream.print(Printed.toSysOut());
        joinStream.to("joinedMsg");

    }
}
