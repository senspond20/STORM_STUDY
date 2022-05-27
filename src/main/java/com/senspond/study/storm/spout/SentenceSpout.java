package com.senspond.study.storm.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout{

//	private String[] sentences;
	private SpoutOutputCollector collector;
	private int index = 0;
	
	private SentenceSpout() {}
	private String[] sentences;
	
	public SentenceSpout(String[] sentences) {
		this.sentences = sentences;
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void nextTuple() {
		
		
		Utils.sleep(100);
		this.collector.emit(new Values(this.sentences[index]));
		this.index++;
		if(index >= sentences.length) this.index = 0;

		try {
			Thread.sleep(5);
		}catch (InterruptedException e) {}
//						
	}
	@Override
	public void ack(Object id) {
		System.out.println("prepare");
		System.out.println(id);
		
	}

	@Override
	public void fail(Object id) {
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
		
	}

}
