package com.sample;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.joda.time.DateTime;

import java.util.Properties;

/**
 * Created by Akshay Harale<akshay.harale@synerzip.com/> on 23/6/16.
 */
public class KafkaProducer {
    public static void main(String[] args){
//        Properties props = new Properties();
//
//        props.put("metadata.broker.list", "localhost:9092");
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        //props.put("partitioner.class", "kafka.producer.ByteArrayPartitioner");
//        props.put("request.required.acks", "1");
//
//        ProducerConfig config = new ProducerConfig(props);
//        Producer<String, String> producer = new Producer<String, String>(config);
//        int i=0;
//        while(true){
//            KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", ""+(i++));
//            producer.send(data);
//        }
        DateTime dateTime = new DateTime();
        System.out.println(dateTime.toString());

    }
}
