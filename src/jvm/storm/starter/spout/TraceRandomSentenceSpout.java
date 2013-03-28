package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: maphysics
 * Date: 3/15/13
 * Time: 9:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class TraceRandomSentenceSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;
    int traceId = 0;
    String _componentId = "";
    HashMap _traceInfo = new HashMap();


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        _componentId = context.getThisComponentId();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        int initTraceCount = traceId;
        String failReason = "";
        String[] sentences = new String[] {
                "the cow jumped over the moon",
                "an apple a day keeps the doctor away",
                "four score and seven years ago",
                "snow white and the seven dwarfs",
                "i am at two with nature"};
        String sentence = sentences[_rand.nextInt(sentences.length)];
        String cow = "cow";
        HashMap trace = new HashMap();
        if (sentence.toLowerCase().contains(cow.toLowerCase())){
            traceId += 1;
            String traceID = Integer.toHexString(traceId);
            trace.put("traceID", traceID);
            trace.put("traceTrail", _componentId);
        }
        String snow = "snow";
        Boolean fail = Boolean.FALSE;
        if (sentence.toLowerCase().contains(snow.toLowerCase())){
            fail = Boolean.FALSE;
            failReason = snow;
        }
        String nature = "frog";
        Boolean die = Boolean.FALSE;
        if (sentence.toLowerCase().contains(nature.toLowerCase())){
            die = Boolean.FALSE;
        }
        if (failReason.equals("")){
            failReason = null;
        }
        if (initTraceCount == traceId){
            trace = null;
        }

        _collector.emit( new Values(die, fail, failReason, trace, sentence));

    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("_die","_fail","failReason","_trace","sentence"));
    }

}
