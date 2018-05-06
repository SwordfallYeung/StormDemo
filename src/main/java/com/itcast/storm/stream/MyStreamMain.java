package com.itcast.storm.stream;

/*import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;*/
import com.itcast.storm.stream.bolt.SaveDataBolt;
import com.itcast.storm.stream.bolt.SplitBolt;
import com.itcast.storm.stream.spout.NumberSpout;
import com.itcast.storm.stream.spout.SignSpout;
import com.itcast.storm.stream.spout.StringSpout;
import com.itcast.storm.stream.streamBolt.NumberStreamBolt;
import com.itcast.storm.stream.streamBolt.SignStreamBolt;
import com.itcast.storm.stream.streamBolt.StringStreamBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author y15079
 * @create 2018-05-06 0:26
 * @desc
 **/
public class MyStreamMain {

	public static void main(String[] args) throws Exception {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		//set spouts 分
		topologyBuilder.setSpout("NumberSpout", new NumberSpout(), 1);
		topologyBuilder.setSpout("StringSpout", new StringSpout(), 1);
		topologyBuilder.setSpout("SignSpout",new SignSpout(),1);

		//set bolts 合
		topologyBuilder.setBolt("SplitBolt", new SplitBolt(), 1)
				.shuffleGrouping("NumberSpout")
				.shuffleGrouping("StringSpout")
				.shuffleGrouping("SignSpout");

		//set bolts 分
		topologyBuilder.setBolt("StringStreamBolt", new StringStreamBolt(), 1)
				.shuffleGrouping("SplitBolt", "string-stream");
		topologyBuilder.setBolt("NumberStreamBolt", new NumberStreamBolt(), 1)
				.shuffleGrouping("SplitBolt","number-stream");
		topologyBuilder.setBolt("SignStreamBolt",new SignStreamBolt(), 1)
				.shuffleGrouping("SplitBolt","sign-stream");

		//set bolt 合
		topologyBuilder.setBolt("SaveDataBolt", new SaveDataBolt(), 3)
				.fieldsGrouping("StringStreamBolt", new Fields("type"))
				.fieldsGrouping("NumberStreamBolt", new Fields("type"))
				.fieldsGrouping("SignStreamBolt", new Fields("type"));

		Config config = new Config();
		String name = MyStreamMain.class.getSimpleName();
		if (args != null && args.length > 0) {
			String nimbus = args[0];
			config.put(Config.NIMBUS_HOST, nimbus);
			config.setNumWorkers(2);
			StormSubmitter.submitTopologyWithProgressBar(name, config, topologyBuilder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, config, topologyBuilder.createTopology());
			Thread.sleep(10 * 10 * 1000);
			cluster.shutdown();
		}
	}
}
