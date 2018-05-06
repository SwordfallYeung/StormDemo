package com.itcast.storm.wordcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/*import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;*/

import java.util.Map;

/**
 * @author y15079
 * @create 2018-05-04 10:17
 * @desc
 **/
public class MySpout extends BaseRichSpout {
	SpoutOutputCollector collector;  //数据流转用的，想象成水管管道

	private String[] sentences = {
			"Apache Storm is a free and open source distributed realtime computation system",
			"Storm makes it easy to reliably process unbounded streams of data",
			"doing for realtime processing what Hadoop did for batch processing",
			"Storm is simple",
			"can be used with any programming language",
			"and is a lot of fun to use" };
	private int index = 0;

	//初始化方法 初始化collector
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
	}

	//storm 框架在while(true)调用nextTuple方法  轮询发送数据,不断发送数据
	public void nextTuple() {
		if (index <= sentences.length - 1){
			//发送字符串
			System.out.println("--------------------------------------------------");
			System.out.println("index: "+index+ " sentences.length"+sentences.length);
			this.collector.emit(new Values(sentences[index]));
			index++;
			Utils.sleep(1);
		}
	}

	//进行声明
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("sentence")); //给刚才准备的数据取个名字，好让后续节点取到对应的数据
	}
}
