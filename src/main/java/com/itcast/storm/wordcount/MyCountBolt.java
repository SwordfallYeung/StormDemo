package com.itcast.storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/*import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;*/

import java.util.HashMap;
import java.util.Map;

/**
 * @author y15079
 * @create 2018-05-04 10:21
 * @desc 第一种写法
 **/
public class MyCountBolt extends BaseRichBolt {
	OutputCollector collector;
	Map<String, Integer> counts = new HashMap<String, Integer>();

	@Override
	public void cleanup() {
		//拓扑结束执行
		for (String key : counts.keySet()) {
			System.out.println(key + " : " + this.counts.get(key));
		}
	}

	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	public void execute(Tuple input) {
		String word = input.getString(0);
		Integer num = input.getInteger(1);
		System.out.println("--------------"+Thread.currentThread().getId() + " word:"+word);
		if (counts.containsKey(word)){
			Integer count = counts.get(word);
			counts.put(word, count + num);
		}else {
			counts.put(word, num);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//不输出
	}
}
/*
第二种写法
public class MyCountBolt extends BaseBasicBolt{
	Map<String, Integer> counts = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counts = new HashMap<String, Integer>();
	}

	public void execute(Tuple input, BasicOutputCollector basicOutputCollector) {
		String word = input.getStringByField("word");
		Integer num = this.counts.get(word);
		if (num == null){
			num = 0;
		}
		num++;
		counts.put(word, num);
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//不输出
	}

	@Override
	public void cleanup() {
		//拓扑结束执行
		for (String key : counts.keySet()) {
			System.out.println(key + " : " + this.counts.get(key));
		}
	}
}*/
