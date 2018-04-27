package io.github.bibekshakya35.githubcommitcounter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import io.github.bibekshakya35.githubcommitcounter.topology.CommitFeedListener;
import io.github.bibekshakya35.githubcommitcounter.topology.EmailCounter;
import io.github.bibekshakya35.githubcommitcounter.topology.EmailExtractor;

public class LocalTopologyRunner {
    private static final int TEN_MINUTES = 600000;
    public static void main(String[] args){
        //Used for wiring together the spout and bolts
        TopologyBuilder builder = new TopologyBuilder();
        //spout is a source of stream
        //add the commits feed listener to the topology
        //with an ID of commit-feed-listener
        builder.setSpout("commit-feed-listener",new CommitFeedListener());
        //add the email extractor to the topology with an ID of email-extractor
        //defines the stream between the commit feed listener and email extractor
        builder.setBolt("email-extractor",new EmailExtractor())
                .shuffleGrouping("commit-feed-listener");
        //add the email counter to the topology with an ID of email counter
        //define the stream between the email extractor and email counter
        builder.setBolt("email-counter",new EmailCounter())
                .fieldsGrouping("email-extractor",new Fields("email"));
        //class for defining topology-level configuration
        Config config = new Config();
        config.setDebug(true);
        //creates the topology
        StormTopology topology = builder.createTopology();
        //Defines a local cluster that can be run in-memory
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("github-commit-count-topology",config,topology);
        Utils.sleep(TEN_MINUTES);
        cluster.killTopology("github-commit-count-topology");
        cluster.shutdown();
    }
}
