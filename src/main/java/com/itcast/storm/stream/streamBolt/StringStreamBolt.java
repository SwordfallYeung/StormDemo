package com.itcast.storm.stream.streamBolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author y15079
 * @create 2018-05-06 0:23
 * @desc
 **/
public class StringStreamBolt extends BaseBasicBolt {
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		for (String word : words) {
			word = word.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				collector.emit( new Values("STRING",word));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("type","word"));
	}
}
