package cca.topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cca.Order;
import cca.dao.OrderDao;
import cca.service.AuthorizationService;

import java.util.Map;

public class AuthorizeCreditCard extends BaseRichBolt {
    private AuthorizationService authorizationService;
    private OrderDao orderDao;
    private OutputCollector outputCollector;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("order"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector outputCollector) {
        authorizationService = new AuthorizationService();
        orderDao = new OrderDao();
        this.outputCollector = outputCollector;

    }


    @Override
    public void execute(Tuple tuple) {
        Order order = (Order) tuple.getValueByField("order");
        try {
            if (orderDao.isSystemAvailable()) {
                if (!orderDao.isNotReadyToShip(order)) {
                    boolean isAuthorized = authorizationService.authorize(order);
                    if (isAuthorized) {
                        orderDao.updateStatusToReadyToShip(order);
                    } else {
                        orderDao.updateStatusToDenied(order);
                    }
                    //anchor to the input tuple
                    outputCollector.emit(tuple, new Values(order));
                }
                //always emit a tuple with the order making sure our external system
                //knows to do something
                outputCollector.emit(tuple, new Values(order));
                //always ack the input tuple if there wasn't an error
                //Ack the input tuple
                outputCollector.ack(tuple);
            } else {
                //fail the tuple if the database is not available
                outputCollector.fail(tuple);
            }
        } catch (Exception e) {
            //failed the input tuple in the case of exception
            outputCollector.fail(tuple);
        }

    }
}
