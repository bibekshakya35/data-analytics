package heatmap.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class HeatmapTopologyBuilder {
    public static StormTopology build(){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("checkins",new Checkins(),4);
        builder.setBolt("geocode-lookup",new GeocodeLookup(),8)
                .setNumTasks(64)
                .shuffleGrouping("checkins");
        builder.setBolt("time-interval-extractor",new TimeIntervalExtractor())
                .shuffleGrouping("geocode-lookup");

        builder.setBolt("heatmap-builder",new HeatMapBuilder())
                .fieldsGrouping("time-interval-extractor",new Fields("time-interval","city"));

        builder.setBolt("persistor",new Persistor(),1)
                .setNumTasks(4)
                .shuffleGrouping("heatmap-builder");
        return builder.createTopology();
    }
}
