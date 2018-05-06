package com.itcast.storm.kafkaAndStorm.orderinfo;

import com.itcast.storm.kafkaAndStorm.orderinfo.OrderInfo;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import kafka.producer.ProducerConfig;


import java.util.Properties;


/**
 * @author y15079
 * @create 2018-05-06 10:32
 * @desc
 **/
public class OrderMqSender {
	public static void main(String[] args) {
		String TOPIC = "orderMq";
		Properties props = new Properties();
		props.put("serializer.class","kafka.serializer.StringEncoder");
		props.put("metadata.broker.list","hadoop1:9092,hadoop2:9092,hadoop3:9092");
		props.put("request.required.acks","1");
		props.put("partitioner.class","kafka.producer.DefaultPartitioner");
		Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
		for (int messageNo = 1;messageNo < 100000; messageNo++){
			producer.send(new KeyedMessage<String, String>(TOPIC, messageNo + "",new OrderInfo().random()));
		}
	}
}
