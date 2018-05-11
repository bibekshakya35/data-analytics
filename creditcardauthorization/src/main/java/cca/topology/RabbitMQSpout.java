package cca.topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cca.Order;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RabbitMQSpout extends BaseRichSpout {
    private Connection connection;
    private Channel channel;
    private QueueingConsumer queueingConsumer;
    private SpoutOutputCollector outputCollector;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("order"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = spoutOutputCollector;
        try {
            //connect to a RabbitMQ node
            //running on localhost with default
            //credentials and settings
            connection = new ConnectionFactory()
                    .newConnection();
            channel = connection.createChannel();
            //consume and locally buffer 25 messages at a time from RabbitMQ
            channel.basicQos(25);
            //when we take messages off a RabbitMQ queue
            //we will buffer them locally inside this consumer
            queueingConsumer = new QueueingConsumer(channel);
            //Set up subscription that consumes off a RabbitMQ queue. The messages
            //consumed will be kept in a local
            //buffer in a consumer object and we
            //won’t ack them until downstream
            //bolts send their acks back
            channel.basicConsume("orders", false/*auto-ack-false*/, queueingConsumer);
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    /**
     * Storm will call next tuple when it’s ready to send the next message to downstream bolt.
     */
    public void nextTuple() {
        try {
            //Pick the next message off the local buffer of the RabbitMQ queue.
            // A timeout of 1 ms is imposed as nextTuple shouldn’t be allowed to
            // block for thread safety issues within Storm
            QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery(1L);
            if (delivery == null) return;/*no message yet*/
            Long msgId = delivery.getEnvelope()
                    .getDeliveryTag();
            byte[] msgAsBytes = delivery.getBody();
            String msgAsString = new String(msgAsBytes, Charset.forName("UTF-8"));
            Order order = new Gson().fromJson(msgAsString, Order.class);
            outputCollector.emit(new Values(order), msgId);
        } catch (InterruptedException e) {

        }
    }

    @Override
    public void ack(Object msgId) {
        try {
            channel.basicAck((long) msgId, false/*only acking this msgId */);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void fail(Object msgId) {
        try {
            channel.basicReject((Long) msgId, true/* requeue enabled*/);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
            connection.close();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }
}
