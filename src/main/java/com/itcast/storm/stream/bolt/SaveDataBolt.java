package com.itcast.storm.stream.bolt;

/*import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;*/

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;


/**
 * @author y15079
 * @create 2018-05-05 23:59
 * @desc
 **/
public class SaveDataBolt extends BaseBasicBolt {
	Map<String, Integer> counters = new HashMap<String,Integer>();

	@Override
	public void execute(Tuple input, BasicOutputCollector basicOutputCollector) {
		String str = input.getString(0);
		if (!counters.containsKey(str)){
			counters.put(str, 1);
		}else {
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
		System.out.println(Thread.currentThread().getId() + " " + counters);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}
}
