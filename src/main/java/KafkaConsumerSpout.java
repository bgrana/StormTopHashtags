import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import storm.kafka.*;

import java.util.Map;
import java.util.UUID;

/**
 * Created by ignacio on 16/12/15.
 */
public class KafkaConsumerSpout extends BaseRichSpout implements KafkaConsumerSpoutConstants{

    private BrokerHosts hosts ;
    private SpoutConfig spoutConfig;
    private KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);


    public void nextTuple() {
        kafkaSpout.nextTuple();
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        hosts = new ZkHosts(ZOOKEEPER_ADDR, KAFKA_BROKER_PATH);
        spoutConfig = new SpoutConfig(hosts, TOPIC_NAME, "/" + TOPIC_NAME, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaSpout = new KafkaSpout(spoutConfig);
        kafkaSpout.open(map,topologyContext,spoutOutputCollector);
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        kafkaSpout.declareOutputFields(outputFieldsDeclarer);
    }
}
