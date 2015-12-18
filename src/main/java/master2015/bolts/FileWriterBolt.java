package master2015.bolts;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWriterBolt extends BaseRichBolt {
	private static final long serialVersionUID = 4493727735986580186L;
	Logger logger = LoggerFactory.getLogger(FileWriterBolt.class);

	private PrintWriter writer;
	private OutputCollector outputCollector;

	private String filename;
	private String dir;

	public FileWriterBolt( String dir, String filename){
		this.filename = filename;
		this.dir = dir;
	}

	public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
		try {
			writer = new PrintWriter(new FileWriter(dir+ "/" + filename, true));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple tuple) {
		writer.println(tuple.getValueByField("line"));
		logger.error(tuple.getValueByField("line").toString());
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
