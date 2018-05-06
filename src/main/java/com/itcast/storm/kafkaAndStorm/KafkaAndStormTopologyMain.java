package com.itcast.storm.kafkaAndStorm;

/*import backtype.storm.topology.TopologyBuilder;*/
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;


/**
 * @author y15079
 * @create 2018-05-06 1:40
 * @desc
 **/
public class KafkaAndStormTopologyMain {

	public static void main(String[] args) throws Exception{
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("kafkaSpout",
				new KafkaSpout(new SpoutConfig(
						new ZkHosts("hadoop1:2181,hadoop2:2181,hadoop3:2181"),
						"orderMq",
						"/myKafka",
						"kafkaSpout"
				)),1);
		topologyBuilder.setBolt("mybolt1", new ParserOrderMqBolt(),1).shuffleGrouping("kafkaSpout");
		//topologyBuilder.setBolt("mybolt1", new MyBolt1(),1).shuffleGrouping("kafkaSpout");

		Config config = new Config();
		config.setNumWorkers(1);

		//3.提交任务 ——两种模式  本地模式和集群模式
		//3、提交任务  -----两种模式 本地模式和集群模式
		if (args.length>0) {
			StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("storm2kafka", config, topologyBuilder.createTopology());
		}
	}
}
