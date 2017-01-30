package com.qat.samples.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.databind.JsonNode;

public class DocCodeReportConsumer extends Thread {
	KafkaConsumer<String, Long> consumer;
	
	public DocCodeReportConsumer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
				"org.apache.kafka.common.serialization.LongDeserializer");
		
		consumer = new KafkaConsumer<>(props);
		
		consumer.subscribe(Arrays.asList("ACountTopic"));
		
		
	}
	
	public void run() {
		consumer.poll(1000);
		System.out.println("DCPoll...");
		while (true) {
			ConsumerRecords<String, Long> records = consumer.poll(1000);
			
			if (!records.isEmpty()) {
				System.out.println("");
				System.out.println("");
				System.out.println("COUNT UPDATE:");
			}
			for (ConsumerRecord<String, Long> record : records) {
				System.out.println("\t" + record.key() + " = " + record.value());
			}
			
			consumer.commitSync();
			
			//new ReportProcessor();
		}
	}
}
