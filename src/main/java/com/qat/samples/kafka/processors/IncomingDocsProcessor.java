package com.qat.samples.kafka.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qat.samples.kafka.model.PatApplication;
import com.qat.samples.kafka.model.PatDocument;
import com.qat.samples.kafka.repo.DocRepository;
import com.qat.samples.kafka.repo.PatRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ProducerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 * Created by rpulley on 2/10/17.
 */
public class IncomingDocsProcessor {

    @Autowired
    ProducerFactory<String, String> producerFactory;

    @Autowired
    ProducerFactory<String, Long> producerLongFactory;


    @Autowired
    DocRepository docRepo;

    @Autowired
    PatRepository patRepo;

    Producer<String, String> producer;

    Producer<String, Long> longProducer;

    @PostConstruct
    public void init() {
        producer = producerFactory.createProducer();
        longProducer = producerLongFactory.createProducer();
    }

    @KafkaListener(id = "foo", topics = "incoming-docs", group = "kafka-stream-demo")
    public void listen(ConsumerRecord<String, String> record) {
        ObjectMapper mapper = new ObjectMapper();

        try {
            PatDocument[] docs = mapper.readValue(record.value().getBytes(), PatDocument[].class);

            PatApplication app = patRepo.save(new PatApplication());

            //System.out.println("Created app: " + app.getApplicationId());

            for (PatDocument doc : docs) {
                doc.setApplicationId(app.getApplicationId());

                doc = docRepo.save(doc);

                //Put in topic for doc code report
                producer.send(new ProducerRecord<>("report-doc-codes", "docCode", doc.getDocCode()));

                //Put in topic for page count report
                longProducer.send(new ProducerRecord<>("report-doc-pages", "pages", Long.valueOf(doc.getNumPages())));

                //Put in topic for message distribution processing
                producer.send(new ProducerRecord<>("message-process", doc.getDocumentId(), mapper.writeValueAsString(doc)));

                if (doc.getDocCode().equals("ABST")) {
                    //send any ABST documents to PASS for processing
                    producer.send(new ProducerRecord<>("pass-process", "doc", doc.getDocumentId()));
                }

                producer.send(new ProducerRecord<>("dav-notification", app.getApplicationId(), doc.getDocumentId()));
            }

        } catch(IOException e) {
            System.out.println("Bad one: " + record.value());
        }
    }
}
