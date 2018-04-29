package heatmap.topology;

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

public class Checkins extends BaseRichSpout  {
    //store the static check-ins from a file in List
    private List<String> checkins;
    //nextEmitIndex will keep track of our
    //current position in the list as we'll
    //recycle the static list of checkins
    private int nextEmitIndex;
    private SpoutOutputCollector outputCollector;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = spoutOutputCollector;
        this.nextEmitIndex = 0;
        try {
            checkins = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("checkins.txt"),
                    Charset.defaultCharset().name());
        }catch (IOException io){
            throw new RuntimeException(io);
        }
    }

    public void nextTuple() {
        //when storm requests the next tuple from the spout,look up
        //the next check-in from our in-memory List and parse it into time and address components
        String checkin =checkins.get(nextEmitIndex);
        String[] parts =checkin.split(",");
        Long time =Long.valueOf(parts[0]);
        String address =parts[1];
        //use the SpoutOutputCollector provided in the spout open method to emit the relevant fields
        outputCollector.emit(new Values(time,address));
        //advance the index of the next item to be emitted (recycling if at the end of the list)
        nextEmitIndex = (nextEmitIndex+1)%checkins.size();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //let storm know that this spout will emit a tuple
        //containing two fields named time and address
        outputFieldsDeclarer.declare(new Fields("time","address"));
    }
}
