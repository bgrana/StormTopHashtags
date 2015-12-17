package master2015.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class RankBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9130467237351920234L;
	private OutputCollector outputCollector;

	public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
    	
    	//outputCollector.emit(new Values(ordered));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("line"));
    }

}
