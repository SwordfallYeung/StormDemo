package com.itcast.storm.ackfail;

/*import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;*/

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author y15079
 * @create 2018-05-05 23:33
 * @desc
 **/
public class MyBolt1 extends BaseRichBolt {
	private OutputCollector collector;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	@Override
	public void execute(Tuple input) {
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		for (String word: words){
			word = word.trim();
			if (!word.isEmpty()){
				word = word.toLowerCase();
				collector.emit(input, new Values(word));
			}
		}
		System.out.println("--------------------------"+sentence);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("word"));
	}
}
