package cca.topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cca.Order;
import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Spout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private List<Order> orderList;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("order"));
    }

    private List<Order> convertJsonList(List<String> jsonList) {
        List<Order> orders = new ArrayList<>();
        Gson gson = new Gson();
        for (String json : jsonList) {
            Order order = gson.fromJson(json, Order.class);
            orders.add(order);
        }
        return orders;
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        try {
            List<String> jsonList = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("orders.txt"),
                    Charset.defaultCharset());
            orderList = convertJsonList(jsonList);
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
    }

    public void nextTuple() {
        for (Order order : orderList) {
            spoutOutputCollector.emit(new Values(order));
        }
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }
}
