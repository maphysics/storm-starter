package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.*;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.utils.Utils;
import storm.starter.spout.TraceRandomSentenceSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import storm.starter.FailerHook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: maphysics
 * Date: 3/12/13
 * Time: 3:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class ReverseStringTopology {
    public static Logger LOG = LoggerFactory.getLogger(TracerHook.class);

    public static class SplitSentenceBolt extends BaseRichBolt {
        OutputCollector _collector;
        String name = "SplitSentenceBolt";

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("backwardsSentence");
            HashMap trace = (HashMap) tuple.getValueByField("_trace");
            String[] words = sentence.split(" ");
            for (int wordIn=0;wordIn < words.length -1;wordIn++){
                _collector.emit(new Values(trace, words[wordIn]));
                _collector.ack(tuple);
            }
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("_trace", "word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class WordCount extends BaseRichBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();
        OutputCollector _collector;

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");
            HashMap trace = (HashMap) tuple.getValueByField("_trace");
            Integer count = counts.get(word);
            if(count==null) count = 0;
            count++;
            counts.put(word, count);
            _collector.emit(new Values(trace, word, count));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("_trace", "word", "count"));
        }
    }

    public static class DieBolt extends BaseRichBolt {
        OutputCollector _collector;

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        public void execute(Tuple tuple) {
            Boolean die = tuple.getBooleanByField("_die");
            String sentence = tuple.getStringByField("sentence");
            String failReason = tuple.getStringByField("failReason");
            if (die){
                System.out.println("Die my pretties!");
                _collector.fail(tuple);
                throw new FailedException(sentence.concat(" is marked FAILED!"));
            }
            else{
                _collector.emit(new Values(tuple.getBooleanByField("_fail"), failReason,
                        tuple.getValueByField("_trace"), sentence));
                _collector.ack(tuple);
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("_fail", "failReason","_trace", "sentence"));
        }
    }

    public static class FailBolt extends BaseRichBolt {
        public static Logger LOG = LoggerFactory.getLogger(FailBolt.class);
        OutputCollector _collector;

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        public void execute(Tuple tuple) {
            Boolean fail = tuple.getBooleanByField("_fail");
            String sentence = tuple.getStringByField("sentence");
            if (fail){
                LOG.info(sentence.concat(": ").concat("Is made of fail!").concat("\n"));
//                throw new ReportedFailedException("sentence marked as failed");
                _collector.fail(tuple);
            }
            else{
                _collector.emit(tuple, new Values(tuple.getBooleanByField("_fail"), tuple.getStringByField("failReason"),
                        tuple.getValueByField("_trace"), sentence));
                _collector.ack(tuple);
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("_fail", "failReason", "_trace", "sentence"));
        }
    }

    public static class ReverseSentence extends BaseRichBolt {
        String name = "ReverseSentence";
        public String ReverseString(String args){
            int i;
            String phrase = args;
            int lenPhrase = phrase.length();
            String reversePhrase = "";
            for (i=lenPhrase;i>0;i--){
                reversePhrase = reversePhrase + phrase.substring(i-1,i);
            }
            return reversePhrase;
        }

        OutputCollector _collector;

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        public void execute(Tuple tuple) {
            Date date = new Date();
            String sentence = tuple.getStringByField("sentence");
            HashMap trace = (HashMap) tuple.getValueByField("_trace");
            String backwardsSentence = "";
            backwardsSentence = ReverseString(sentence);
            _collector.emit(tuple, new Values(trace, backwardsSentence));
            _collector.ack(tuple);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("_trace", "backwardsSentence"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new TraceRandomSentenceSpout(), 5);

        builder.setBolt("my_die", new DieBolt(), 3)
                .fieldsGrouping("spout", new Fields("_die"));

        builder.setBolt("my_fail", new FailBolt(), 3)
                .fieldsGrouping("my_die", new Fields("_fail"));

        builder.setBolt("reverse", new ReverseSentence(), 3)
                .shuffleGrouping("my_fail");
        builder.setBolt("split", new SplitSentenceBolt(), 3)
                .shuffleGrouping("reverse");
        builder.setBolt("wordCount", new WordCount(), 3)
                .shuffleGrouping("split");

        List hooks = new ArrayList<String>();
        hooks.add("storm.starter.TracerHook");
//        hooks.add("storm.starter.FailerHook");
//        hooks.add("storm.starter.DierHook");
        Config conf = new Config();
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_AUTO_TASK_HOOKS,hooks);
        conf.registerMetricsConsumer(storm.starter.SourceMetric.class, "source", 3);


        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            String hostname = (String) conf.get(Config.NIMBUS_HOST);
            System.out.println(hostname);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("my_topology", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("my_topology");
            cluster.shutdown();
        }
    }
}
