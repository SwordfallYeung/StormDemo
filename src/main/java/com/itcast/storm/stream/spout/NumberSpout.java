package com.itcast.storm.stream.spout;

/*import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;*/

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * @author y15079
 * @create 2018-05-06 0:16
 * @desc
 **/
public class NumberSpout extends BaseRichSpout {
	SpoutOutputCollector collector;
	Random random;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("content"));
	}

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
		this.random = new Random();
	}

	@Override
	public void nextTuple() {
		String[] sentences = new String[]{
				"111 222 333 4444 555 666",
				"111 222 333 4444 555 666",
				"111 222 333 4444 555 666",
				"111 222 333 4444 555 666",
				"111 222 333 4444 555 666"};
		String sentence = sentences[random.nextInt(sentences.length)];
		String messageId = UUID.randomUUID().toString().replace("-", "");
		collector.emit(new Values(sentence), messageId);
		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
