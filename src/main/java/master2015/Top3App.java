package master2015;

import java.util.UUID;

import backtype.storm.Config;
//import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
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
    public final static String GROUP_ID ="04";

	private static String outputDir;

    public static void main(String[] args){
    	
    	if (args.length != 5) {
    		System.out.println("Wrong number of arguments");
    		System.exit(1);
    	}

		String[] langList = args[0].split(",");
		String zkURL = args[1];
		String[] window = args[2].split(",");
    	long size = Long.parseLong(window[0]);
    	long slide = Long.parseLong(window[1]);
		String topologyName = args[3];
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
        	
        	builder.setBolt("write-" + lang, new FileWriterBolt(outputDir,lang + "_"+ GROUP_ID + ".log"))
        		.localOrShuffleGrouping("rank-" + lang);
        }

		//TODO Comment, only for testing purposes.
//		  LocalCluster cluster = new LocalCluster();
//		  cluster.submitTopology(topologyName, new Config(), builder.createTopology());

        Config config = new Config();
        config.setNumWorkers(2);
        try {
            StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
			e.printStackTrace();
		}
    }
}
