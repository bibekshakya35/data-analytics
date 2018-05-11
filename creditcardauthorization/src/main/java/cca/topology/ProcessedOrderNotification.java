package cca.topology;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import cca.Order;
import cca.service.NotificationService;

import java.util.Map;

public class ProcessedOrderNotification extends BaseBasicBolt {
    private NotificationService notificationService;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.notificationService = new NotificationService();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //nothing to declare
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Order order = (Order) tuple
                .getValueByField("order");
        this.notificationService.notifyOrderHasBeenProcessed(order);
    }
}
