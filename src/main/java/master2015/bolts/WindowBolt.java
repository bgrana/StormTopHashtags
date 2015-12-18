package master2015.bolts;

import java.util.*;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by ignacio on 16/12/15.
 */
public class WindowBolt extends BaseRichBolt {
	private static final long serialVersionUID = 7734630606574453501L;
	private OutputCollector collector;

	private long size, slide, count;
	private long ts0;

	//List related
	private TreeMap<Long,List<String>> window;

	public WindowBolt(long size, long slide) {
		this.size = size;
		this.slide = slide;
		this.count = 0;
		this.window = new TreeMap<Long,List<String>>();
	}

	public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		ts0 = System.currentTimeMillis();
    }

    public void execute(Tuple tuple) {
		//Parsing the tuple;
		String[] array = tuple.getValueByField("str").toString().split(";");
		long ts = (Long.valueOf(array[0])-ts0) / 1000;
		String hashtag = array[1];

		//TODO remove, debug
		//System.out.println("Hashtag: "+ hashtag + ", FIXED_TS:" + ts);
		List<String> hashtags = window.get(ts);
		if(hashtags == null){
			hashtags = new ArrayList<String>(71);
			window.put(ts,hashtags);
		}
		hashtags.add(hashtag);

		if (ts > count + size){ //Send the window to the next bolt
			Long key0 = window.higherKey(count);
			Long key1 = window.lowerKey(count+size);
			count = count + slide;
			if( key0 != null && key1 != null) {
				Collection<List<String>> submap = window.subMap(key0, key1).values();
				if(submap.size()>0){
					LinkedList<String> totalColl = new LinkedList<String>();
					for (List hs_list : submap){
						 totalColl.addAll(hs_list);
					}
					collector.emit( new Values( count * 1000, totalColl ) );
					//TODO remove, debug
					//System.out.println( count * 1000 + "," + totalColl );
				}
			}
			window = new TreeMap<Long,List<String>>(window.subMap(window.higherKey(count),window.lastKey()));
		}
	}

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("timestamp" , "hashtags"));
    }

}
