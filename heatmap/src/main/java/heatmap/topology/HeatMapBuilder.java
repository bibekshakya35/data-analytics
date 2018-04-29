package heatmap.topology;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.code.geocoder.model.LatLng;

import java.util.*;

public class HeatMapBuilder extends BaseBasicBolt {
    private Map<Long, List<LatLng>> heatmaps;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        heatmaps = new HashMap<Long, List<LatLng>>();
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if (isTickTuple(tuple)) {
            emitHeatmap(basicOutputCollector);
        } else {
            Long time = tuple.getLongByField("time-interval");
            LatLng geocode = (LatLng) tuple.getValueByField("geocode");
            Long timeInterval = selectTimeInterval(time);
            List<LatLng> checkins = getCheckinsForInterval(timeInterval);
            checkins.add(geocode);
        }
    }

    private List<LatLng> getCheckinsForInterval(Long timeInterval) {
        List<LatLng> hotzones = heatmaps.get(timeInterval);
        if (hotzones == null) {
            hotzones = new ArrayList<LatLng>();
            heatmaps.put(timeInterval, hotzones);
        }
        return hotzones;
    }

    private Long selectTimeInterval(Long time) {
        return time / (15 * 1000);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time-interval"
        ,"hotzones"));
    }

    private boolean isTickTuple(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        String sourceStreamId = tuple.getSourceStreamId();
        return sourceComponent.equalsIgnoreCase(Constants.SYSTEM_COMPONENT_ID)
                && sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private void emitHeatmap(BasicOutputCollector outputCollector) {
        Long now = System.currentTimeMillis();
        Long emitUpToTimeInterval = selectTimeInterval(now);
        Set<Long> timeIntervalsAvailable = heatmaps.keySet();
        for (Long timeInterval : timeIntervalsAvailable) {
            if(timeInterval<=emitUpToTimeInterval){
                List<LatLng> hotzones = heatmaps.remove(timeInterval);
                outputCollector.emit(new Values(timeInterval, hotzones));
            }
        }
    }

    /**
     * overriding this method allows us to configure various
     * aspects of how our component runs in this case, setting the tick tuple frequency
     *
     * @return
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
        return config;
    }

}
