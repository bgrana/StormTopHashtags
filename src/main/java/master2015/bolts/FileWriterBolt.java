package master2015.bolts;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class FileWriterBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4493727735986580186L;
	private PrintWriter writer;
	private OutputCollector outputCollector;

	private String filename;

	public FileWriterBolt(String filename){
		this.filename = filename;
	}

	public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
		try {
			writer = new PrintWriter(filename, "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple tuple) {
		writer.println(tuple);
		writer.flush();
		outputCollector.ack(tuple);

	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}

	@Override
	public void cleanup() {
		writer.close();
		super.cleanup();

	}

}
