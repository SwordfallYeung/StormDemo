package com.itcast.storm.kafkaAndStorm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author y15079
 * @create 2018-05-06 16:15
 * @desc
 **/
public class MyBolt1 extends BaseRichBolt {
	OutputCollector collector;
	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	@Override
	public void execute(Tuple input) {
		String string = new String((byte[]) input.getValue(0));
		System.out.println(string);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}
}
