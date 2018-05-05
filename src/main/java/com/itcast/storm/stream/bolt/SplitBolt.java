package com.itcast.storm.stream.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author y15079
 * @create 2018-05-06 0:05
 * @desc
 **/
public class SplitBolt extends BaseBasicBolt{
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String message = input.getString(0);
		if (message.contains("1")){
			collector.emit("number-stream", new Values(message));
		}
		if (message.contains("t")){
			collector.emit("string-stream", new Values(message));
		}
		if (message.contains("!")){
			collector.emit("sing-stream", new Values(message));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declareStream("string-stream", new Fields("String"));
		outputFieldsDeclarer.declareStream("sign-stream", new Fields("sing"));
		outputFieldsDeclarer.declareStream("number-stream", new Fields("number"));
	}
}
