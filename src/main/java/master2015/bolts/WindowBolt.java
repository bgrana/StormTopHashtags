package master2015.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by ignacio on 16/12/15.
 */
public class WindowBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = 7734630606574453501L;
	private double size, slide, count;
	
	public WindowBolt(double size, double slide) {
		this.size = size;
		this.slide = slide;
		this.count = 0;
	}

	public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {
    	System.out.println(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
