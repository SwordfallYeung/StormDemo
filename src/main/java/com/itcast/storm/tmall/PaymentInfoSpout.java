package com.itcast.storm.tmall;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author y15079
 * @create 2018-05-06 15:54
 * @desc
 **/
public class PaymentInfoSpout extends BaseRichSpout {
	private static final String TOPIC = "paymentInfo";
	private Properties props;
	private ConsumerConnector consumer;
	private SpoutOutputCollector collector;
	//ArrayBlockingQueue是一个由数组支持的有界阻塞队列。此队列按 FIFO（先进先出）原则对元素进行排序。
	// 队列的头部 是在队列中存在时间最长的元素
	private ArrayBlockingQueue<String> paymentInfoQueue = new ArrayBlockingQueue<String>(100);

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;

		props = new Properties();
		props.put("zookeeper.connect", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
		props.put("group.id","testGroup");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TOPIC, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()){
			paymentInfoQueue.add(new String(it.next().message()));
		}
	}

	@Override
	public void nextTuple() {
		try {
			collector.emit(new Values(paymentInfoQueue.take()));
			try {
				Thread.sleep(1000);
			} catch (Exception e) {

			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("paymentInfo"));
	}
}
