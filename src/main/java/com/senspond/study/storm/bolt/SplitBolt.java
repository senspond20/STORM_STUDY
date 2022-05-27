package com.senspond.study.storm.bolt;

import java.text.BreakIterator;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//There are a variety of bolt types. In this case, use BaseBasicBolt
public class SplitBolt extends BaseBasicBolt {

	private final String FILED = "sentence";
	
//	OutputCollector collector;
		
//	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//		this.collector = outputCollector;
//	}
	
//	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String sentence = input.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for(String word : words) {
			if (!word.equals("")) {
		        collector.emit(new Values(word));
		      }
		}
	}

//	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	
}
