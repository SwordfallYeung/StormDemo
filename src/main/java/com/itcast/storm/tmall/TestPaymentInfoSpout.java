package com.itcast.storm.tmall;

import com.itcast.storm.tmall.other.PaymentInfo;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * @author y15079
 * @create 2018-05-06 15:41
 * @desc
 **/
public class TestPaymentInfoSpout extends BaseRichSpout{
	private SpoutOutputCollector collector;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("paymentInfo"));
	}

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
	}

	@Override
	public void nextTuple() {
		collector.emit(new Values(new PaymentInfo().random()));
		Utils.sleep(10);
		/**
		 * 继去年创下当天1261万笔支付的世界纪录后，支付宝在今年光棍节再度刷新这一纪录，创下当天支付成功3369万笔，
		 * 比去年纪录增长了近170%！其中无线支付达到171万笔！11日0:01分，支付宝在一分钟内的付款笔数瞬间超过5.5万笔，
		 * 是去年峰值的2.5倍。
		 * 衷心感谢所有消费者、商家与银行与我们共同完成这一奇迹！
		 * http://weibo.com/1627897870/xx1sduqnC?type=comment
		 */
	}
}
