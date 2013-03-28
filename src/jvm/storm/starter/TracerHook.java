package storm.starter;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.task.TopologyContext;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.tuple.Fields;
import backtype.storm.generated.Bolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

/**
 * Created with IntelliJ IDEA.
 * User: maphysics
 * Date: 3/20/13
 * Time: 12:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class TracerHook extends BaseTaskHook {

    Logger LOG = LoggerFactory.getLogger(TracerHook.class);
    private HashMap allInfo = new HashMap();

    public void getBasicBoltExecuteInfo(BoltExecuteInfo info){
        allInfo.put("tuple", info.tuple);
        allInfo.put("trace", info.tuple.getValueByField("_trace"));
        HashMap trace = (HashMap) allInfo.get("trace");
        allInfo.put("traceID", trace.get("traceID"));
        allInfo.put("traceTrail", trace.get("traceTrail"));
    }

    public void getBoltExecuteInfo(BoltExecuteInfo info){
        getBasicBoltExecuteInfo(info);
        allInfo.put("fields", info.tuple.getFields());
        allInfo.put("values", info.tuple.getValues());
    }

    public void getPrepareInfo(java.util.Map conf, TopologyContext context, String streamName){
            allInfo.put("componentID", context.getThisComponentId());
            if (allInfo.get("componentID") != "__acker"){
                allInfo.put("streams", context.getThisStreams());
                allInfo.put("sources", context.getThisSources());
                allInfo.put("outputFields", context.getThisOutputFields(streamName));
                allInfo.put("targets", context.getThisTargets());
            }
            else{
                allInfo.put("sources", "from __acker");
                allInfo.put("outputfields", "from __acker");
                allInfo.put("targets", "from __acker");
            }
    }

    public String messageMake(Vector<String> allInfoKeys, String component){
        String logString = "from step: " + component + ": ";
        Iterator it = allInfo.keySet().iterator();
        while(it.hasNext()) {
            String key = (String) it.next();
            if (allInfoKeys.contains(key)){
                Object val = (Object) allInfo.get(key);
                logString = logString.concat(key + ": " + val + "\t");
            }
        }
        return logString;
    }

    public void output(String logstring){
        LOG.info(logstring);
    }

    public void updateTraceTrail(String component){
        HashMap trace = (HashMap) allInfo.get("trace");
        String traceTrail = (String) trace.get("traceTrail");

        traceTrail = component + "(" + traceTrail + ")";
        allInfo.put("traceTrail", traceTrail);
        trace.put("traceTrail", traceTrail);
        allInfo.put("newTrace", trace);
    }

    public void updateEmitTraceTrail(String component, EmitInfo info){
        updateTraceTrail((String) allInfo.get("componentID"));

        Tuple tuple = (Tuple) allInfo.get("tuple");
        Integer traceIndex = tuple.fieldIndex("_trace");

        output("from updateEmitTraceTrail in component= " + allInfo.get("componentID") + " info.values at traceIndex = "
                + traceIndex + " is: " + info.values.get(traceIndex) + " values are: " + info.values);
        output("from component: " + component+ " traceInfoIndex: " + traceIndex +
                " and info.value.size() " + info.values.size() + " and tuple.size() " + tuple.size() +
                " values are: " + info.values);



//        info.values.remove(traceIndex);
//        info.values.add(traceIndex, allInfo.get("newTrace"));

    }

    @Override
    public void prepare(java.util.Map conf, TopologyContext context){
        getPrepareInfo(conf, context, "default");
        Vector elements = new Vector();
        elements.add("componentID");
        elements.add("sources");
        elements.add("outputFields");
        elements.add("targets");
        elements.add("streams");
        output(messageMake(elements, allInfo.get("componentID") + ": prepare"));

    }

    public void emit(EmitInfo info){
        //May duplicate trail names, not sure
        //__system streams don't have the same information because they don't have tuples for instance
        //so those records need to be skipped or handle differently
        if (! allInfo.get("componentID").equals("spout") && allInfo.get("_trace") != null){
//            output("from emit componentID: " + allInfo.get("componentID") + " values: " + allInfo.get("values") +
//                    " trace: " + allInfo.get("trace"));
//            output("from updateemitTraceTrail" + allInfo.keySet());
            updateEmitTraceTrail((String) allInfo.get("componentID"), info);
            Vector elements = new Vector();
            elements.add("traceID");
            elements.add("traceTrail");
            output(messageMake(elements, allInfo.get("componentID") + ": emit"));
        }
    }

    public void boltExecute(BoltExecuteInfo info){
            if (info.tuple.getValueByField("_trace") != null && !allInfo.get("componentID").equals("spout")){
                getBoltExecuteInfo(info);
                Vector elements = new Vector();
                elements.add("traceID");
                elements.add("fields");
                elements.add("values");
                output(messageMake(elements, allInfo.get("componentID") + ": boltExecute"));
            }
    }
}
