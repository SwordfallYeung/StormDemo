package com.itcast.storm.ackfail;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author y15079
 * @create 2018-05-05 23:09
 * @desc ack/fail机制保证数据不丢失
 **/
public class MyAckFailTopology {
	public static void main(String[] args) throws Exception{
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("mySpout", new MySpout(), 1);
		topologyBuilder.setBolt("mybolt1", new MyBolt1(), 1).shuffleGrouping("mySpout");

		Config config = new Config();
		String name = MyAckFailTopology.class.getSimpleName();
		if (args != null && args.length > 0){
			String nimbus = args[0];
			config.put(Config.NIMBUS_HOST, nimbus);
			config.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar(name, config, topologyBuilder.createTopology());
		}else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, config, topologyBuilder.createTopology());
			Thread.sleep(60 * 60 * 1000);
			cluster.shutdown();
		}
	}
}
