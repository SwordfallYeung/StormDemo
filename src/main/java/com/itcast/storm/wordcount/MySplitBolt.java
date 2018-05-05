package com.itcast.storm.wordcount;

/*import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;*/

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * @author y15079
 * @create 2018-05-04 10:26
 * @desc 第一种写法
 **/
public class MySplitBolt extends BaseRichBolt {
	OutputCollector collector;

	//初始化方法
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	// 被storm框架 while(true) 循环调用  传入参数tuple
	public void execute(Tuple input) {
		String line = input.getString(0);  //获取上一个环节的数据
		String[] arrWords = line.split(" ");
		for (String word:arrWords){
			collector.emit(new Values(word,1)); //发送到下一个bolt
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("word","num")); //同样还是取别名
	}
}

/*
第二种写法
public class MySplitBolt extends BaseBasicBolt {

	public void execute(Tuple input, BasicOutputCollector collector) {
		String sentence = input.getStringByField("sentence");  //获取上一个环节的数据
		String[] words = sentence.split(" ");
		for (String word : words){
			collector.emit(new Values(word)); //发送到下一个bolt
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("word")); //同样还是取别名
	}
}*/
