package com.qat.samples.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Created by rpulley on 2/10/17.
 */
public class DocCodeCountProcessor {

    @Resource(name="docCodeStreamConfig")
    Map<String, Object> docCodeStreamConfig;

    @PostConstruct
    public void init() {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id");
        // Where to find Kafka broker(s).
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Specify default (de)serializers for record keys and for record values.
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2 * 1000);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> docCodesStream = builder.stream(Serdes.String(), Serdes.String(), "report-doc-codes");

        docCodesStream.flatMapValues(value -> Arrays.asList(value))
                .groupBy((key, docCode) -> docCode).count("DocCodeCount").toStream().to(Serdes.String(), Serdes.Long(), "report-doc-code-count");

        KafkaStreams kstream = new KafkaStreams(builder, props);
        kstream.start();

        System.out.println("Started stream processor");
    }

    @KafkaListener(id = "foo2", topics = "report-doc-code-count", group = "kafka-stream-long-demo", containerFactory = "stringLongFactory")
    public void listen(ConsumerRecord<String, Long> record) {
        System.out.println("Doc Code: " + record.key() + " = " + record.value());
    }
}
