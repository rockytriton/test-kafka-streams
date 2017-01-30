package com.qat.samples.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class IncomingDocProcessor extends Thread {
	KafkaConsumer<String, JsonNode> consumer;
	KafkaProducer<String, String> producer;
	KafkaProducer<String, String> reportDocCodesProducer;
	KafkaProducer<String, Long> reportPagesProducer;
	ObjectMapper mapper = new ObjectMapper();

	public IncomingDocProcessor() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
				"org.apache.kafka.connect.json.JsonDeserializer");
		
		
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		consumer = new KafkaConsumer<>(props);
		producer = new KafkaProducer<>(props);
		
		//props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");
		
		reportDocCodesProducer = new KafkaProducer<>(props);
		
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");
		
		reportPagesProducer = new KafkaProducer<>(props);
	    
		consumer.subscribe(Arrays.asList("incoming-docs"));
	}

	public void run() {
		consumer.poll(1000);
		System.out.println("Polling...");
		while (true) {
			ConsumerRecords<String, JsonNode> records = consumer.poll(1000);
			for (ConsumerRecord<String, JsonNode> record : records) {
				PatDocument[] docs = getDocuments(record.value());
				
				System.out.println("IncomingDocs Recv: " + docs);
				
				processDocuments(record.key(), docs);
			}
			
			consumer.commitSync();
			
			//new ReportProcessor();
		}
	}
	
	private void processDocuments(String submissionData, PatDocument[] docs) {
		String[] vars = submissionData.split("-");
		
		for (PatDocument doc : docs) {
			doc.setSourceSystem(vars[0]);
			doc.setSubmissionId(vars[1]);
			
			try {
				String json = mapper.writeValueAsString(doc);
				producer.send(new ProducerRecord<String, String>("process-docs", submissionData, json));
				reportDocCodesProducer.send(new ProducerRecord<String, String>("report-doc-codes-3", doc.getDocCode(), doc.getDocCode()));
				reportPagesProducer.send(new ProducerRecord<String, Long>("report-page-count", doc.getDocCode(), new Long(doc.getNumPages())));
				
			} catch(JsonProcessingException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	private PatDocument[] getDocuments(JsonNode jsonNode) {
		try {
			return mapper.treeToValue(jsonNode, PatDocument[].class);
		} catch(JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
}
