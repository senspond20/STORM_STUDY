package com.senspond.study.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import ch.qos.logback.classic.pattern.Util;

public class ReportBolt extends BaseRichBolt{

	private HashMap<String, Long> counter = null;
	
//	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.counter = new HashMap<String, Long>();
	}

//	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = input.getLongByField("count");
		this.counter.put(word, count);
//		Utils.sleep(100);
	}

//	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	@Override
	public void cleanup() {
		System.out.println("=============== COUNT ==============");
		
		for(String key : this.counter.keySet()) {
			System.out.println(key + " : " +  this.counter.get(key));
		}
		System.out.println("===================================");
	}

}
