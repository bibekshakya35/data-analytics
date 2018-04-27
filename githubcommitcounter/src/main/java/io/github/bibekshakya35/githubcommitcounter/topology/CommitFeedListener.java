package io.github.bibekshakya35.githubcommitcounter.topology;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * This spout simulates reading commits from a live stream by doing two things
 * </p>
 * 1) Reading a file containing commit data into a list of strings (one string per commit)
 * 2) When nextTuple() is called, emit a tuple for each string in the list
 */
public class CommitFeedListener extends BaseRichSpout{
    private SpoutOutputCollector outputCollector;
    private List<String> commits;


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = spoutOutputCollector;
        try{
            commits = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("changelog.txt"),
                    Charset.defaultCharset().name());
        }catch (IOException io){
            throw new RuntimeException(io);
        }
    }

    @Override
    public void nextTuple() {
        for (String commit:
             commits) {
            outputCollector.emit(new Values(commit));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("commit"));
    }
}
