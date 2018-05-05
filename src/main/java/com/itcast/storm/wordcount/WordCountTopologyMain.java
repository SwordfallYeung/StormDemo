package com.itcast.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.itcast.storm.wordcount.MyCountBolt;
import com.itcast.storm.wordcount.MySplitBolt;
import com.itcast.storm.wordcount.MySpout;
/*
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
*/

/**
 * @author y15079
 * @create 2018-05-04 9:44
 * @desc
 **/
public class WordCountTopologyMain {

	public static void main(String[] args) throws Exception{

		//1、准备一个TopologyBuilder
		TopologyBuilder topologyBuilder = new TopologyBuilder();  //拓扑构建
		topologyBuilder.setSpout("mySpout",new MySpout(), 2);  //设置数据来源
		topologyBuilder.setBolt("mybolt1",new MySplitBolt(),2).shuffleGrouping("mySpout");  //数据处理
		topologyBuilder.setBolt("mybolt2",new MyCountBolt(),4).fieldsGrouping("mybolt1",new Fields("word")); //数据处理

		//2、创建一个configuration，用来指定当前topology需要的worker的数量

		Config config = new Config();  //配置
		config.setDebug(false);
		config.setNumWorkers(2);       //两个worker，也就是两个jvm

		//3、提交任务 —— 两种模式 本地模式和集群模式
		//StormSubmitter.submitTopology("myWordCount",config, topologyBuilder.createTopology());  //集群模式 提交拓扑结构，开始运行

		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("myWordCount", config, topologyBuilder.createTopology());  //本地模式
		Thread.sleep(20000);
		localCluster.shutdown();
	}
}
