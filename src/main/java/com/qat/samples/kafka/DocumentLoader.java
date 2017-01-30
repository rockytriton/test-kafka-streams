package com.qat.samples.kafka;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DocumentLoader {
	public static void main(String[] args) throws Exception {
		KafkaProducer<String, String> producer;
		ObjectMapper mapper = new ObjectMapper();
		Properties props = new Properties();
		
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<>(props);
		
		String sourceSystem = "EFSWeb";
		String submissionId = "SUB_" + System.currentTimeMillis();
		
		PatDocument doc1 = new PatDocument();
		doc1.setDocCode("CLM");
		doc1.setOfficialDate(new Date());
		doc1.setNumPages(5);
		doc1.setSourceId(submissionId + "-0");
		
		PatDocument doc2 = new PatDocument();
		doc2.setDocCode("SPEC");
		doc2.setOfficialDate(new Date());
		doc2.setNumPages(50);
		doc2.setSourceId(submissionId + "-1");
		
		PatDocument doc3 = new PatDocument();
		doc3.setDocCode("ABST");
		doc3.setOfficialDate(new Date());
		doc3.setNumPages(1);
		doc3.setSourceId(submissionId + "-2");
		
		String json = mapper.writeValueAsString(Arrays.asList(doc1, doc2, doc3));
		
		producer.send(new ProducerRecord<String, String>("incoming-docs", 
				sourceSystem + "-" + submissionId, json));
		
		producer.flush();
		
	    producer.close();
	    
	}
}
