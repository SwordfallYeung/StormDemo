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
 * @create 2018-05-06 0:19
 * @desc
 **/
public class SignSpout extends BaseRichSpout {
	SpoutOutputCollector collector;
	Random random;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("content"));
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.random = new Random();
	}

	@Override
	public void nextTuple() {
		String[] sentences = new String[]{
				"!!! @@@ ### $$$ %%% ^^^",
				"!!! @@@ ### $$$ %%% ^^^",
				"!!! @@@ ### $$$ %%% ^^^",
				"!!! @@@ ### $$$ %%% ^^^",
				"!!! @@@ ### $$$ %%% ^^^"};
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
