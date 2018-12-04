package heatmap;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import heatmap.topology.HeatmapTopologyBuilder;

public class LocalTopologyRunner {
    public static void main(String[] args) {
        Config config = new Config();
        StormTopology topology
                = HeatmapTopologyBuilder.build();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("local-heatmap", config, topology);
    }
}
