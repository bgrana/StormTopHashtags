package master2015.bolts;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CountBolt extends BaseRichBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5166346793801758346L;
	private OutputCollector outputCollector;

	public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {

    	@SuppressWarnings("unchecked")
		Collection<String> hashtags = (Collection<String>) tuple.getValueByField("hashtags");
    	Long timestamp = (Long) tuple.getValueByField("timestamp");
    	Map<String, Integer> frequencies = new HashMap<String, Integer>();
    	Integer count = 0;
    	
    	for (String hashtag : hashtags) {
    		count = frequencies.get(hashtag);
    		if (count == null) {
    			count = 0;
    		}
    		frequencies.put(hashtag, ++count);
    	}
    	
    	outputCollector.emit(new Values(frequencies, timestamp));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("frequencies", "timestamp"));
    }
}
