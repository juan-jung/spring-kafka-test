package com.example.springkafkapract.service;

import com.example.springkafkapract.vo.EffectOrNot;
import com.example.springkafkapract.vo.PurchaseLog;
import com.example.springkafkapract.vo.WatchingAdLog;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

@Service
public class AdEvaluationService {
    //광고 데이터는 중복 join될 필요없음. --> ktable
    //광고 이력이 먼저 들어온다. (구매 후 광고를 본 것은 평가x)
    //구매 이력은 상품별로 들어오지 않는다 (한번의 주문번호에 복수 개의 상품 존재 --> contain)
    //광고 시청 시간(머문시간)이 적어도  10초 이상되어야만 join 대상
    //특정 가격 이상의 상품은 join 대상 제외 ( 고가의 상품은 광고 효과가 미미하다고 가정)
    //광고이력 : KTable(AdLog), 구매이력: KStream(OrderLog)
    //filtering, 형변환
    // .EffectOrNot -> Json형태로 Topic(EvaluationComplete)에 전달

    @Autowired
    public void buildPipeline(StreamsBuilder sb) {
        JsonSerializer<EffectOrNot> effectOrNotJsonSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLog> purchaseLogJsonSerializer = new JsonSerializer<>();
        JsonSerializer<WatchingAdLog> watchingAdLogJsonSerializer = new JsonSerializer<>();

        JsonDeserializer<EffectOrNot>  effectOrNotJsonDeserializer = new JsonDeserializer<>();
        JsonDeserializer<PurchaseLog>  purchaseLogJsonDeserializer = new JsonDeserializer<>();
        JsonDeserializer<WatchingAdLog>  watchingAdLogJsonDeserializer = new JsonDeserializer<>();

        Serde<EffectOrNot> effectOrNotSerde = Serdes.serdeFrom(effectOrNotJsonSerializer, effectOrNotJsonDeserializer);
        Serde<PurchaseLog> purchaseLogSerde = Serdes.serdeFrom(purchaseLogJsonSerializer, purchaseLogJsonDeserializer);
        Serde<WatchingAdLog> watchingAdLogSerde = Serdes.serdeFrom(watchingAdLogJsonSerializer, watchingAdLogJsonDeserializer);

        //adLog stream --> table
        KTable<String, WatchingAdLog> adTable =
                sb.stream("AdLog", Consumed.with(Serdes.String(), watchingAdLogSerde))
                        .toTable(Materialized.<String, WatchingAdLog, KeyValueStore<Bytes, byte[]>>as("adStore"));


    }

}
