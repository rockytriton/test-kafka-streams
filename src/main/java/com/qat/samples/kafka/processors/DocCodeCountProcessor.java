package com.qat.samples.kafka.processors;

import com.qat.samples.kafka.model.DocCodeReportItem;
import com.qat.samples.kafka.repo.DocCodeReportRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
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
@Slf4j
public class DocCodeCountProcessor {

    @Resource(name="docCodeStreamConfig")
    Map<String, Object> docCodeStreamConfig;

    @Autowired
    DocCodeReportRepository reportRepo;

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.putAll(docCodeStreamConfig);

        KStreamBuilder builder = new KStreamBuilder();
        
        KStream<String, String> docCodesStream = builder.stream(Serdes.String(), Serdes.String(), "report-doc-codes");

        docCodesStream.flatMapValues(value -> Arrays.asList(value))
                .groupBy((key, docCode) -> docCode).count("DocCodeCount").toStream().to(Serdes.String(), Serdes.Long(), "report-doc-code-count");

        KafkaStreams kstream = new KafkaStreams(builder, props);
        kstream.start();

        log.info("Started DocCode Processor");
    }

    @KafkaListener(id = "foo2", topics = "report-doc-code-count", group = "kafka-stream-long-demo", containerFactory = "stringLongFactory")
    public void listen(ConsumerRecord<String, Long> record) {
        DocCodeReportItem item = new DocCodeReportItem();
        item.setDocCode(record.key());
        item.setCount(record.value());

        reportRepo.save(item);

        log.info("DocCode: " + record.key() + " = " + record.value());
    }
}
