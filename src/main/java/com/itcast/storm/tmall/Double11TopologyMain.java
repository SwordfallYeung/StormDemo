package com.itcast.storm.tmall;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author y15079
 * @create 2018-05-06 15:39
 * @desc
 * 程序说明:
 * 根据双十一当天的订单mq，快速计算当天的订单量、销售金额
 * 思路：
 * 1,支付系统发送mq到kafka集群中，编写storm程序消费kafka的数据并计算实时的订单数量、订单数量
 * 2,将计算的实时结果保存在redis中
 * 3,外部程序实时展示结果
 * 程序设计
 * 数据产生：编写kafka数据生产者，模拟订单系统发送mq
 * 数据输入：使用PaymentSpout消费kafka中的数据
 * 数据计算：使用CountBolt对数据进行统计
 * 数据存储：使用Sava2RedisBolt对数据进行存储
 * 数据展示：编写java app客户端，对数据进行展示，展示方式为打印在控制台。
 **/
public class Double11TopologyMain {
	public static void main(String[] args) throws Exception {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("readPaymentInfo", new TestPaymentInfoSpout(), 1);
		topologyBuilder.setBolt("processIndex", new FilterMessageBlot(), 2).shuffleGrouping("readPaymentInfo");
		topologyBuilder.setBolt("saveResult2Redis", new Save2RedisBolt(), 2).shuffleGrouping("processIndex");

		Config conf = new Config();
		conf.setDebug(false);
		if (args != null && args.length > 0) {
			conf.setNumWorkers(2);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topologyBuilder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("double11", conf, topologyBuilder.createTopology());
//			Utils.sleep(10000);
//			cluster.shutdown();
		}
	}
}
