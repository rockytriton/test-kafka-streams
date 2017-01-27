package com.qat.samples.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReportProcessor extends Thread {
	KafkaConsumer<String, JsonNode> consumer;
	KafkaProducer<String, String> producer;
	KafkaProducer<String, String> reportProducer;
	ObjectMapper mapper = new ObjectMapper();

	public ReportProcessor() {
		Properties props = new Properties();
		
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "application-reset-demo");
	    // Where to find Kafka broker(s).
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	    // Where to find the corresponding ZooKeeper ensemble.
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
	    // Specify default (de)serializers for record keys and for record values.
		//props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		//props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	    // Read the topic from the very beginning if no previous consumer offsets are found for this app.
	    // Resetting an app will set any existing consumer offsets to zero,
	    // so setting this config combined with resetting will cause the application to re-process all the input data in the topic.
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
"org.apache.kafka.common.serialization.StringSerializer");
				
				//"org.apache.kafka.connect.json.JsonDeserializer");
		
		
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		
		//consumer = new KafkaConsumer<>(props);
		//producer = new KafkaProducer<>(props);
		//reportProducer = new KafkaProducer<>(props);
	    
		//consumer.subscribe(Arrays.asList("incoming-docs"));
		
		KStreamBuilder builder = new KStreamBuilder();

        KStream<String, Long> prices = builder.stream("report-docs");
        
        prices.groupByKey().count("test-count").toStream().to("count-topic");
        
        KafkaStreams kstream = new KafkaStreams(builder, props);
        kstream.start();
        
        
        
	}
	
	public void run() {
		
	}
}
