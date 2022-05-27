package com.senspond.study.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt{

	OutputCollector collector;
	private HashMap<String, Long> counter = null;
	
//	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.counter = new HashMap<String, Long>();
	}

//	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = this.counter.get(word);
		System.out.println(word +  " : " + count);
		count = (Long) (count == null ? 1L : count + 1);
		this.counter.put(word, count);
		this.collector.emit(new Values(word,count));
	}

//	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}

}
