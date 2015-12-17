package master2015.bolts;

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
	private Map<String, Integer> counter;

	public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
    	String hashtag = (String) tuple.getValueByField("hashtag");
    	Integer count = counter.get(hashtag);
    	if (count == null){
    		count = 0;
    	}
    	counter.put(hashtag, ++count);
    	outputCollector.emit(new Values(hashtag, count));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("hashtag", "count"));
    }
}
