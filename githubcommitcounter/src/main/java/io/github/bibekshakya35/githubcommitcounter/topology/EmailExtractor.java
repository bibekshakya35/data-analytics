package io.github.bibekshakya35.githubcommitcounter.topology;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EmailExtractor extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String commit = tuple.getStringByField("commit");
        if(!commit.isEmpty()){
            String[] parts = commit.split(" ");
            basicOutputCollector.emit(new Values(parts[1]));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("email"));
    }
}
