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
    
    private static String[] langList;
    private static String zkURL;
    private static String window;
    private static String topologyName;
    private static String outputDir;

    public static void main(String[] args){
    	
    	if (args.length != 5) {
    		System.out.println("Wrong number of arguments");
    		System.exit(1);
    	}
    	
    	langList = args[0].split(",");
    	zkURL = args[1];
    	window = args[2];
    	topologyName = args[3];
    	outputDir = args[4];
    	
        TopologyBuilder builder= new TopologyBuilder();
        
        for(String lang : langList) {
        	builder.setSpout("spout-" + lang, new KafkaConsumerSpout());
        	builder.setBolt("bolt-" + lang, new TestBolt())
            .localOrShuffleGrouping("spout-" + lang);
        }

        //Esto habría que cambiarlo, ya no es local mode
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, new Config(), builder.createTopology());

        //En teoria esto se queda funcionando para siempre.

    }
}
