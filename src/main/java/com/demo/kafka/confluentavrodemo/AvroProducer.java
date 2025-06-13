package com.demo.kafka.confluentavrodemo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.pack.ClickRecord;

import io.confluent.kafka.serializers.KafkaAvroSerializer;


public class AvroProducer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092");
		
		//key-string value-avro format
		prop.put("key.serializer", StringSerializer.class);
		
		prop.put("value.serializer",KafkaAvroSerializer.class);
		
		//who understands avro format - schema register
		 prop.put("schema.registry.url", "http://localhost:8081");
		
		Producer<String,ClickRecord> producer = new KafkaProducer<>(prop);
		
		//in kafka producer we want to send click record 
		ClickRecord cr = new ClickRecord();
		
		try {
			//send information for click record
			cr.setSessionId("10001");
			cr.setChannel("Homepage");
			cr.setIp("192.168.0.1");
			
			//topicname,cr.sessionid -key , click record - value ,get for sending synchronously
			producer.send(new ProducerRecord<String, ClickRecord>("topicavro",cr.getSessionId().toString(),cr)).get();
			
			System.out.println("complete");
		}catch(Exception e){
			e.printStackTrace();
		}finally {
			producer.close();
		}
		

	}

}
