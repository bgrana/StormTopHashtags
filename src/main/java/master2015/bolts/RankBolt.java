package master2015.bolts;

import java.text.Collator;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import master2015.utils.SerializableCollator;
import scala.Int;

public class RankBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9130467237351920234L;
	private OutputCollector outputCollector;
	private String lang;
	private SerializableCollator coll; //

	public RankBolt(String lang) {
		this.lang = lang;
		coll = new SerializableCollator();
	}

	private String[] getTop3(final Map<String, Integer> frequencies){
		String[] top3 = new String[3];
		Comparator comp = new Comparator<String>() {
			public int compare(String a, String b) {
				int aV = frequencies.get(a);
				int bV = frequencies.get(b);
				if ( aV > bV) {
					return 1;
				} else if ( aV < bV){
					return -1;
				} else {
					return -coll.compare(a,b);
				}
			}
		};

		TreeMap<String,Integer> sortedMap = new TreeMap<String,Integer>(comp);
		sortedMap.putAll(frequencies);

		Map.Entry<String, Integer> entry;
		for( int i = 0; i < top3.length; i++ ){
			entry = sortedMap.pollLastEntry();
			if(entry != null && entry.getKey() != "" && entry.getKey() != null ){
				top3[i] = entry.getKey() + "," + entry.getValue();
			}else{
				top3[i] = "null,0";
			}
		}
		//TODO debug, remove
		//System.out.print(top3[0] + "," + top3[1] + "," + top3[2]);
		return top3;

	}

	public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
    	Long timestamp = (Long) tuple.getValueByField("timestamp");
    	@SuppressWarnings("unchecked")
		Map<String, Integer> frequencies = (HashMap<String, Integer>) tuple.getValueByField("frequencies");
		//TODO remove, debug
		//System.out.println(frequencies.toString());
    	String[] top3Keys = getTop3(frequencies);
		String line = timestamp + "," + lang + "," + top3Keys[0] + "," + top3Keys[1] + "," + top3Keys[2];
		outputCollector.emit(new Values(line));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("line"));
    }

}
