package com.senspond.study.storm.topology;

import com.senspond.study.storm.bolt.ReportBolt;
import com.senspond.study.storm.bolt.SplitBolt;
import com.senspond.study.storm.bolt.WordCountBolt;
import com.senspond.study.storm.spout.SentenceSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountReportTopology {

	static final String SENTENCE_SPOUT_ID = "sentence-spout";
	static final String SPLIT_BOLT_ID = "split-bolt";
	static final String COUNT_BOLT_ID = "count-bolt";
	static final String REPORT_BOLT_ID = "report-bolt";
	static final String TOPOLOGY_NAME = "word-count-topology";
	
	static String[] sentences = new String[] {
			"빅데이터 자바 파이썬",
			"안드로이드 휴먼 AI",
			"빅데이터 플랫폼 엘라스틱 서치 키바나 로그스태쉬 자바"
	};
	
	public static void main(String[] args) {

		SentenceSpout sentenceSpout = new SentenceSpout(sentences);
		
		SplitBolt splitBolt = new SplitBolt();
		WordCountBolt wordCountBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(SENTENCE_SPOUT_ID, sentenceSpout);
		
		builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);  // spout -> split-bolt
		builder.setBolt(COUNT_BOLT_ID, wordCountBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word")); // split-bolt -> count-bolt
		builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID); // count-bolt -> report-bolt;
		
		
		
		LocalCluster cluster = new LocalCluster();
	    Config conf = new Config();
//	    conf.setDebug(false);
//	    conf.setNumWorkers(3);

	      
		cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
//		try {
//			Thread.sleep(1000 * 20);
//		}catch (InterruptedException e) {
//			// TODO: handle exceptio
//	
//		}finally {
//		}
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
		
		
	}
}
