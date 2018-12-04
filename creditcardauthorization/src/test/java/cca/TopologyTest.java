package cca;

import backtype.storm.Config;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
import cca.topology.AuthorizeCreditCard;
import cca.topology.ProcessedOrderNotification;
import cca.topology.Spout;
import cca.topology.VerifyOrderStatus;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import java.util.Map;

public class TopologyTest {
    @Test
    public void verifyProperValuesAreEmittedByEachBolt(){
        Config config = new Config();
        config.setDebug(false);
        MkClusterParam clusterParam = new MkClusterParam();
        clusterParam.setSupervisors(1);
        clusterParam.setDaemonConf(config);
        Testing.withSimulatedTimeLocalCluster(clusterParam,cluster -> {
            MockedSources mockedSources = new MockedSources();
            mockedSources.addMockData("file-based-spout",
                    new Values(new Order(1234, 5678, 1111222233334444L, "012014", 123, 42.23)));

            Config config1 = new Config();
            config1.setDebug(true);

            CompleteTopologyParam topologyParam =new CompleteTopologyParam();
            topologyParam.setMockedSources(mockedSources);
            topologyParam.setStormConf(config1);

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("file-based-spout",new Spout());
            builder.setBolt("verify-order-status",new VerifyOrderStatus())
                    .shuffleGrouping("file-based-spout");

            builder.setBolt("authorize-order",new AuthorizeCreditCard())
                    .shuffleGrouping("verify-order-status");

            builder.setBolt("accepted-notification",new ProcessedOrderNotification())
                    .shuffleGrouping("authorize-order");
            StormTopology topology = builder.createTopology();
            Map result = Testing.completeTopology(cluster,topology,topologyParam);
            assertEquals(3,result.size());
        });
    }
}

