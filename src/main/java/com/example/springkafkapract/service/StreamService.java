package com.example.springkafkapract.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
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
        KStream<String, String> left =  sb.stream("leftTopic", Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, String> right =  sb.stream("rightTopic", Consumed.with(STRING_SERDE, STRING_SERDE));

        ValueJoiner<String, String, String> stringJoiner = (leftValue, rightValue) -> {
            return "[StringJoiner] " + leftValue + "-" + rightValue;
        };

        ValueJoiner<String, String, String> stringOuterJoiner = (leftValue, rightValue) -> {
            return "[stringOuterJoiner] " + leftValue + "<" + rightValue;
        };

        KStream<String, String> joinStream = left.join(
                right,
                stringJoiner,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1)));

        KStream<String, String> outerJoinStream = left.join(
                right,
                stringOuterJoiner,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1)));

        joinStream.print(Printed.toSysOut());
        joinStream.to("joinedMsg");
        outerJoinStream.to("joinedMsg");

    }
}
