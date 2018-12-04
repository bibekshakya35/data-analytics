package heatmap.topology;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.code.geocoder.model.LatLng;

public class TimeIntervalExtractor extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Long time = tuple.getLongByField("time");
        LatLng geocode = (LatLng)tuple.getValueByField("geocode");
        String city = tuple.getStringByField("city");
        Long timeInterval = time/(15*1000);
        basicOutputCollector.emit(new Values(timeInterval,geocode,city));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time-interval", "geocode","city"));
    }
}
