package com.demo.kafka.confluentavrodemo;

import java.util.Arrays;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.pack.ClickRecord;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class AvroConsumer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092");
		
		//key-string value-avro format
		prop.put("key.deserializer", StringDeserializer.class);
		
		prop.put("value.deserializer",KafkaAvroDeserializer.class);
		
		//when we need consumer consumer group is mandatory
		prop.put("group.id", "RG1");

		prop.put("schema.registry.url", "http://localhost:8081");
		
		//
		
		prop.put("specific.avro.reader", true);
		
		KafkaConsumer<String,ClickRecord> consumer = new KafkaConsumer<>(prop);
		
		consumer.subscribe(Arrays.asList("topicavro"));
		
		try {
			
			while(true) {
				ConsumerRecords<String, ClickRecord> records = consumer.poll(100);
				for(ConsumerRecord<String, ClickRecord> record: records) {
					System.out.println("session id: "+record.value().getSessionId()+" Channel: "+record.value().getChannel()+" Referrer: "+record.value().getReferrer());;
				}
			}
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			consumer.close();
		}
	}

}
