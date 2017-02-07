package com.qat.samples.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReportProcessorPages extends Thread {
	KafkaConsumer<String, JsonNode> consumer;
	KafkaProducer<String, String> producer;
	KafkaProducer<String, String> reportProducer;
	ObjectMapper mapper = new ObjectMapper();

	public ReportProcessorPages() {
		Properties props = new Properties();
		
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "application-reset-demo");
	    // Where to find Kafka broker(s).
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	    // Where to find the corresponding ZooKeeper ensemble.
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
	    // Specify default (de)serializers for record keys and for record values.
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	    // Read the topic from the very beginning if no previous consumer offsets are found for this app.
	    // Resetting an app will set any existing consumer offsets to zero,
	    // so setting this config combined with resetting will cause the application to re-process all the input data in the topic.
		//props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		
		//consumer = new KafkaConsumer<>(props);
		//producer = new KafkaProducer<>(props);
		//reportProducer = new KafkaProducer<>(props);
	    
		//consumer.subscribe(Arrays.asList("incoming-docs"));
		
		KStreamBuilder builder = new KStreamBuilder();

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStream<String, String> docPagesStream = builder.stream(stringSerde, stringSerde, "report-doc-codes");
        
        docPagesStream.flatMapValues(value -> Arrays.asList(value))
        	.groupBy((key, pages) -> key).count("ACountTest").toStream().to(stringSerde, longSerde, "ACountTopic");
        
        
        
        KafkaStreams kstream = new KafkaStreams(builder, props);
        kstream.start();
        
        
        
	}
	
	public void run() {
		
	}
}
