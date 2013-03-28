package storm.starter;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.*;
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

    public void getBoltAckInfo(BoltAckInfo info){
        allInfo.put("ackingTaskID", info.ackingTaskId);
        allInfo.put("processLatencyMs", info.processLatencyMs);
    }
    public void getBoltFailInfo(BoltFailInfo info){
        allInfo.put("failingTaskID", info.failingTaskId);
        allInfo.put("failLatencyMs", info.failLatencyMs);
    }

    public void getBasicBoltExecuteInfo(BoltExecuteInfo info){
        allInfo.put("tuple", info.tuple);
        allInfo.put("trace", info.tuple.getValueByField("_trace"));
        allInfo.put("executeLatencyMs", info.executeLatencyMs);
        HashMap trace = (HashMap) allInfo.get("trace");
        allInfo.put("traceID", trace.get("traceID"));
        allInfo.put("traceMessage", trace.get("traceMessage"));
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

    public String messageMake(Vector<String> allInfoKeys, String component, String step){
        String logString = "from " + step + " step of componentID: " + component + ": ";
        Iterator it = allInfo.keySet().iterator();
        while(it.hasNext()) {
            String key = (String) it.next();
            if (allInfoKeys.contains(key)){
                Object val = (Object) allInfo.get(key);
                logString = logString.concat(key + ": " + val + "\t");
            }
        }
        logString.concat("\n");
        return logString;
    }

    public void output(String str){
        LOG.info(str);
    }

    public void updateTraceTrail(String extendString){
        HashMap trace = (HashMap) allInfo.get("trace");
        String traceMessage = (String) trace.get("traceMessage");

        traceMessage = extendString + "(" + traceMessage + ")";
        trace.put("traceMessage", traceMessage);
        allInfo.put("newTrace", trace);
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
        output(messageMake(elements, (String) allInfo.get("componentID"), "prepare"));

    }

    public void emit(EmitInfo info){
        //May duplicate trail names, not sure
        //__system streams don't have the same information because they don't have tuples for instance
        //so those records need to be skipped or handle differently
        if ((!allInfo.get("componentID").equals("my_spout")) && (allInfo.get("trace") != null)){
            Fields outputFields = (Fields) allInfo.get("outputFields");
            Integer traceIndex = outputFields.fieldIndex("_trace");
            updateTraceTrail((String) allInfo.get("componentID"));

            info.values.add(traceIndex, allInfo.get("newTrace"));

            Vector elements = new Vector();
            elements.add("trace");
            output(messageMake(elements, (String) allInfo.get("componentID"), "emit"));
        }
    }

    @Override
    public void boltAck(BoltAckInfo info) {
//        allInfo.remove("newTrace");
        if ((!allInfo.get("componentID").equals("my_spout")) && (allInfo.get("trace") != null)){
            getBoltAckInfo(info);

            Vector elements = new Vector();
            elements.add("ackingTaskID");
            elements.add("processLatencyMs");
            output(messageMake(elements, (String) allInfo.get("componentID"), "boltAck"));
        }
    }

    @Override
    public void boltFail(BoltFailInfo info) {
        if ((!allInfo.get("componentID").equals("my_spout")) && (allInfo.get("trace") != null)){
            getBoltFailInfo(info);

            Vector elements = new Vector();
            elements.add("failingTaskID");
            elements.add("failLatencyMs");
            output(messageMake(elements, (String) allInfo.get("componentID"), "boltFail"));
        }
    }

    public void boltExecute(BoltExecuteInfo info){
            if ((info.tuple.getValueByField("_trace") != null) && (!allInfo.get("componentID").equals("my_spout"))){
                getBoltExecuteInfo(info);


                Vector elements = new Vector();
                elements.add("executeLatencyMs");
                elements.add("traceID");
                elements.add("fields");
                elements.add("values");
                output(messageMake(elements, (String) allInfo.get("componentID"), "boltExecute"));
            }
    }
}
