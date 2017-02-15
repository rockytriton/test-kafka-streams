package com.qat.samples.kafka;

import java.util.*;

import com.qat.samples.kafka.model.PatDocument;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DocumentLoader {

	static List<String> docCodes = Arrays.asList("SPEC", "CLM", "ABST", "REM", "DRW", "IDS");

	public static void main(String[] args) throws Exception {
		KafkaProducer<String, String> producer;
		ObjectMapper mapper = new ObjectMapper();
		Properties props = new Properties();
		
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-stream-demo");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<>(props);
		
		String sourceSystem = "EFSWeb";

		Random rand = new Random(System.currentTimeMillis());

		for (int i=0; i<35000; i++) {
			int r = rand.nextInt(5) + 1;
			Thread.sleep(1);
			String submissionId = "SUB_" + System.currentTimeMillis();

			List<PatDocument> docs = new ArrayList<>();

			for (int d=0; d<r; d++) {
				PatDocument doc = new PatDocument();
				doc.setNumPages(rand.nextInt(50) + 1);
				doc.setOfficialDate(new Date());
				doc.setSourceId(submissionId + "-" + d);
				doc.setDocCode(docCodes.get(rand.nextInt(docCodes.size())));

				docs.add(doc);
			}

			String json = mapper.writeValueAsString(docs);

			producer.send(new ProducerRecord<>("incoming-docs",
					sourceSystem + "-" + submissionId, json));

		}

		producer.flush();

		System.out.println("DONE");
		
	    producer.close();
	    
	}
}
