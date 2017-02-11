package com.qat.samples.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ProducerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Created by rpulley on 2/10/17.
 */
public class IncomingDocsProcessor {

    public final CountDownLatch countDownLatch1 = new CountDownLatch(1);

    @Autowired
    ProducerFactory<String, String> producerFactory;

    Producer<String, String> prod;

    @PostConstruct
    public void init() {
        prod = producerFactory.createProducer();


    }

    @KafkaListener(id = "foo", topics = "incoming-docs", group = "kafka-stream-demo")
    public void listen(ConsumerRecord<?, ?> record) {
        ObjectMapper mapper = new ObjectMapper();

        try {
            PatDocument[] docs = mapper.readValue(record.value().toString().getBytes(), PatDocument[].class);

            for (PatDocument doc : docs) {
                prod.send(new ProducerRecord<String, String>("report-doc-codes", doc.getDocCode(), doc.getDocCode()));

                //System.out.println("Added doc code: " + doc.getDocCode());
            }

        } catch(IOException e) {
            System.out.println("Bad one: " + record.value());
        }

        countDownLatch1.countDown();
    }
}
