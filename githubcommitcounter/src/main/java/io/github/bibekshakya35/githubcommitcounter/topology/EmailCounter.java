package io.github.bibekshakya35.githubcommitcounter.topology;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class EmailCounter extends BaseBasicBolt {
    private Map<String,Integer> counts;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String email = tuple.getStringByField("email");
        counts.put(email,countFor(email)+1);
        printCounts();
    }

    private void printCounts() {
        for (String email :counts.keySet()){
            System.out.println(String.format("%s has count of %s",email,counts.get(email)));
        }
    }

    private Integer countFor(String email) {
        Integer count =counts.get(email);
        return count==null?0:count;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //this bold doesnot emit any values and therefore doesnot define any output field
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        counts = new HashMap<>();
    }

}
