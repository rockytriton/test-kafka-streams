package com.qat.samples.kafka.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.kafka.annotation.KafkaListener;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by rpulley on 2/13/17.
 */
@Slf4j
public class PageCountProcessor {

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        Map<String, Object> propsMap = new HashMap<>();

        propsMap.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id-pc");
        propsMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsMap.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        propsMap.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        propsMap.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        propsMap.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2 * 1000);

        props.putAll(propsMap);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, Long> pageStream = builder.stream(Serdes.String(), Serdes.Long(), "report-doc-pages");

        pageStream.
                selectKey((k, v) -> k). //this key is always "pages"
                groupByKey(). //groups items by the key
                reduce((v1, v2) -> v1 + v2, "pages-sum"). //reduce and sum into table pages-sum
                to("report-doc-page-count"); //convert table to topic


        KafkaStreams kstream = new KafkaStreams(builder, props);
        kstream.start();


        log.info("Started PageCount Processor (" + kstream.toString() + ")");
    }

    @KafkaListener(id = "foo4", topics = "report-doc-page-count", group = "kafka-stream-long-demo", containerFactory = "stringLongFactory") //group = "kafka-stream-long2-demo", containerFactory = "consumerLongLongFactory") //, containerFactory = "consumerLongFactory")
    public void listen(ConsumerRecord<String, Long> record) {
        //DocCodeReportItem item = new DocCodeReportItem();
        //item.setDocCode(record.key());
        //item.setCount(record.value());

        log.info("Doc Pages: " + record.key() + " = " + record.value());
    }
}
