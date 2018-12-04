package cca;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import cca.topology.AuthorizeCreditCard;
import cca.topology.ProcessedOrderNotification;
import cca.topology.Spout;
import cca.topology.VerifyOrderStatus;

public class LocalTopologyRunner {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("file-base-spout", new Spout());
        builder.setBolt("verify-order-status", new VerifyOrderStatus())
                .shuffleGrouping("file-base-spout");
        builder.setBolt("authorize-order", new AuthorizeCreditCard())
                .shuffleGrouping("verify-order-status");
        builder.setBolt("accepted-notification", new ProcessedOrderNotification())
                .shuffleGrouping("authorize-order");
        Config config = new Config();
        config.setDebug(true);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("credit-card-topology", config,
                builder.createTopology());
        Utils.sleep(600000);
        localCluster.shutdown();
    }
}
