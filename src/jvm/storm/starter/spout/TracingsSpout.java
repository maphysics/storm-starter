package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import storm.starter.SourceObject;
//import storm.starter.ReverseStringBaseTopology;

import java.util.Map;
import java.util.Random;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: maphysics
 * Date: 3/18/13
 * Time: 10:48 AM
 * To change this template use File | Settings | File Templates.
 */
public class TracingsSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;
    Date timestamp;
    String source;
    Boolean _trace;

    public TracingsSpout(SourceObject sourceObj){
        timestamp = sourceObj.getTimestamp();
        source = sourceObj.getSource();
        _trace = sourceObj.get_trace();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();

    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        _collector.emit( new Values(timestamp, source,_trace));
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date","source","_trace"));
    }
}
