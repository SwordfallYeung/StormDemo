package com.itcast.storm.tmall;

import com.google.gson.Gson;
import com.itcast.storm.tmall.other.PaymentInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * @author y15079
 * @create 2018-05-06 15:47
 * @desc
 **/
public class FilterMessageBlot extends BaseBasicBolt {
	@Override
	public void execute(Tuple input, BasicOutputCollector basicOutputCollector) {
		//读取订单数据
		String paymentInfoStr = input.getStringByField("paymentInfo");
		//将订单数据解析成JavaBean
		PaymentInfo paymentInfo = new Gson().fromJson(paymentInfoStr, PaymentInfo.class);
		//过滤订单时间，如果订单时间在2015.11.11这天才进入下游开始计算
		Date date = paymentInfo.getCreateOrderTime();
		if (Calendar.getInstance().get(Calendar.DAY_OF_MONTH) != 31) {
			return;
		}
		basicOutputCollector.emit(new Values(paymentInfoStr));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("message"));
	}
}
