package master2015;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import master2015.bolts.TestBolt;
import master2015.spouts.KafkaConsumerSpout;

/**
 * Created by ignacio on 16/12/15.
 */
public class Top3App {
    static KafkaConsumerSpout kafkaSpout;

    public static void main(String[] args){
        TopologyBuilder builder= new TopologyBuilder();
        kafkaSpout= new KafkaConsumerSpout();
        builder.setSpout("kafkaSpout", kafkaSpout);

        //We have to edit this
        builder.setBolt("bolt1", new TestBolt())
                .localOrShuffleGrouping("kafkaSpout");


        //Esto habría que cambiarlo, ya no es local mode
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Top3Topology", new Config(), builder.createTopology());

        //En teoria esto se queda funcionando para siempre.

    }
}
