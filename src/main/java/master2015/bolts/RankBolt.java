package master2015.bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RankBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9130467237351920234L;
	private OutputCollector outputCollector;
	private String lang;
	
	public RankBolt(String lang) {
		this.lang = lang;
	}
	
	private String[] getTop3(Map<String, Integer> frequencies) {
		
		String first = "";
		String second = "";
		String third = "";
		Integer firstFreq, secondFreq, thirdFreq, keyFreq;

		for (String key : frequencies.keySet()) {
			firstFreq = frequencies.get(first);
			secondFreq = frequencies.get(second);
			thirdFreq = frequencies.get(third);
			keyFreq = frequencies.get(key);
			
			if (firstFreq == null) {
				first = key;
			}			
			else if (firstFreq <= keyFreq) {
				if (firstFreq == keyFreq && key.compareTo(first) < 0) {
					third = second;
					second = key;
				}
				else {
					third = second;
					second = first;
					first = key;
				}
			}
			else if (secondFreq <= keyFreq) {
				if (secondFreq == keyFreq && key.compareTo(second) < 0) {
					third = second;
					second = key;
				}
				else {
					third = second;
					second = key;
				}
			}
			else if (thirdFreq <= keyFreq) {
				if (thirdFreq == keyFreq && key.compareTo(third) < 0) {
					third = second;
					second = key;
				}
				else {
					third = key;
				}
			}
		}
		return new String[]{first, second, third};
	}
	
	private String format(String[] top3Keys, Map<String, Integer> frequencies) {
		return top3Keys[0] + "," + frequencies.get(top3Keys[0]) + ","
				+ top3Keys[1] + "," + frequencies.get(top3Keys[1]) + ","
				+ top3Keys[2] + "," + frequencies.get(top3Keys[2]);
	}

	public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
    	
    	Long timestamp = (Long) tuple.getValueByField("timestamp");
    	@SuppressWarnings("unchecked")
		Map<String, Integer> frequencies = (HashMap<String, Integer>) tuple.getValueByField("frequencies");
    	String[] top3Keys = getTop3(frequencies);
    	String line = timestamp + "," + lang + "," + format(top3Keys, frequencies);
    	outputCollector.emit(new Values(line));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("line"));
    }

}
