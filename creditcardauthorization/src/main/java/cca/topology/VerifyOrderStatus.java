package cca.topology;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cca.Order;
import cca.dao.OrderDao;

import java.util.Map;

public class VerifyOrderStatus extends BaseBasicBolt {
    private OrderDao orderDao;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        orderDao = new OrderDao();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Order order = (Order) tuple.getValueByField("order");
        if (orderDao.isNotReadyToShip(order)) {
            basicOutputCollector.emit(new Values(order));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("order"));
    }
}
