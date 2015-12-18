package master2015;

import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import master2015.bolts.CountBolt;
import master2015.bolts.FileWriterBolt;
import master2015.bolts.RankBolt;
import master2015.bolts.WindowBolt;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * Created by ignacio on 16/12/15.
 */
public class Top3App {
    
    private static String[] langList;
    private static String zkURL;
    private static String[] window;
    private static String topologyName;
    private static String outputDir;

    public static void main(String[] args){
    	
    	if (args.length != 5) {
    		System.out.println("Wrong number of arguments");
    		System.exit(1);
    	}
    	
    	langList = args[0].split(",");
    	zkURL = args[1];
    	window = args[2].split(",");
    	long size = Long.parseLong(window[0]);
    	long slide = Long.parseLong(window[1]);
    	topologyName = args[3];
    	outputDir = args[4];
    	
        TopologyBuilder builder= new TopologyBuilder();
        ZkHosts hosts;
        SpoutConfig spoutConfig;
        
        for(String lang : langList) {

        	hosts = new ZkHosts(zkURL);
        	spoutConfig = new SpoutConfig(hosts, lang, "/" + lang, UUID.randomUUID().toString());
        	spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        	
        	builder.setSpout("spout-" + lang, new KafkaSpout(spoutConfig));
        	
        	builder.setBolt("window-" + lang, new WindowBolt(size, slide))
        		.localOrShuffleGrouping("spout-" + lang);
        	
        	builder.setBolt("count-" + lang, new CountBolt())
        		.localOrShuffleGrouping("window-" + lang);
        	
        	builder.setBolt("rank-" + lang, new RankBolt(lang))
        		.localOrShuffleGrouping("count-" + lang);
        	
        	builder.setBolt("write-" + lang, new FileWriterBolt(lang + "_04.log"))
        		.localOrShuffleGrouping("rank-" + lang);
        }

        //Esto habría que cambiarlo, ya no es local mode
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, new Config(), builder.createTopology());

        //En teoria esto se queda funcionando para siempre.

    }
}
