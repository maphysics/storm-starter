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


    private HashMap allInfo = new HashMap();

    private String traceTrail;
    private String traceTrailNew;
    int traceinfoInt = -1;
    HashMap traceInfoNew = new HashMap();

//    public Boolean isTracerRecord(Tuple tuple){
//        Object trace = (Object) tuple.getValueByField("_trace");
//        if (trace ==){
//            return true;
//        }
//        else{
//            return false
//        }
//    }

    public void getBasicBoltExecuteInfo(BoltExecuteInfo info){
        allInfo.put("tuple", info.tuple);
        allInfo.put("trace", info.tuple.getValueByField("_trace"));
        HashMap traceInfo = (HashMap) allInfo.get("_trace");

        allInfo.put("traceID", traceInfo.get("traceID"));
        allInfo.put("traceTrail", traceInfo.get("traceTrail"));
    }

    public void getBoltExecuteInfo(BoltExecuteInfo info){
        getBasicBoltExecuteInfo(info);
        allInfo.put("fields", info.tuple.getFields());
        allInfo.put("values", info.tuple.getValues());
    }

    public void getPrepareInfo(java.util.Map conf, TopologyContext context, String streamName){
        allInfo.put("componentID", context.getThisComponentId());
        if (allInfo.get("componentID") != "__acker"){
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

    public String messageMake(Vector<String> allInfoKeys){
        String logString = "";
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
        Logger LOG = LoggerFactory.getLogger(TracerHook.class);
        LOG.info(logstring);
    }

    public void updateTraceTrail(String component){
        output("from updateTraceTrail" + allInfo.keySet());
//        if (allInfo.keySet().contains("tuple")){
//            Tuple tuple = (Tuple) allInfo.get("tuple");
//        }
//        else{
//            System.out.println("from updateTraceTrail, allInfo does not contain \"tuple\"");
//        }
//        HashMap tInfo = (HashMap) tuple.getValueByField("traceInfo");
//        String tTrail = (String) tInfo.get("traceTrail");
//        tTrail = component + "(" + tTrail + ")";
//        tInfo.put("traceTrail", tTrail);
//        allInfo.put("traceTrail", tTrail);
//        allInfo.put("newTraceInfo", tInfo);
    }

    public void updateEmitTraceTrail(String component, EmitInfo info){
        output("from updateemitTraceTrail" + allInfo.keySet());
//        updateTraceTrail((String) allInfo.get("componentID"));
//        HashMap trace = (HashMap) allInfo.get("trace");
//        Tuple tuple = (Tuple) allInfo.get("tuple");
//        Integer traceIndex = tuple.fieldIndex("_trace");
//
//        System.out.println("trace: " + trace + " tuple:" + tuple);


//        try{
//            if ((Boolean) allInfo.get("trace") == true){
//                Tuple tuple = (Tuple) allInfo.get("tuple");
//                Integer traceInfoIndex = tuple.fieldIndex("traceInfo");
//                System.out.println("from component: " + component+ " traceInfoIndex: " + traceInfoIndex +
//                        " and info.value.size() " + info.values.size() + " and tuple.size() " + tuple.size() +
//                        " values are: " + info.values);
//                if (info.values.get(traceInfoIndex) != null){
//                    if (info.values.size() - tuple.size() == 1){
//                        info.values.remove(traceInfoIndex);
//                        info.values.add(traceInfoIndex, allInfo.get("newTraceInfo"));
//                    }
//                    else{
//                        Integer delta = info.values.size() - tuple.size();
//
//                    }
//                }
//                else{
//                    System.out.print("traceInfoIndex points to a null value" + traceInfoIndex);
//                }
//            }
//        }
//        catch(Exception name){
//            System.out.println(name + ": from updateEmitTraceTrail probably fine?");
//        }
    }

    @Override
    public void prepare(java.util.Map conf, TopologyContext context){
        getPrepareInfo(conf, context, "default");
        Vector elements = new Vector();
        elements.add("componentID");
        elements.add("sources");
        elements.add("outputFields");
        elements.add("targets");
        output(messageMake(elements));
    }

    public void emit(EmitInfo info){
        //May duplicate trail names, not sure
        if (allInfo.get("componentID") != "__system" && allInfo.get("componentID") != "spout"){
            output("from emit componentID: " + allInfo.get("componentID") + " values: " + allInfo.get("values") +
                    " trace: " + allInfo.get("trace"));
            if (allInfo.get("trace") != new HashMap() && allInfo.get("trace") != null){
                output("from emit trace inner: " + allInfo.get("trace"));
                output("from updateemitTraceTrail" + allInfo.keySet());
//                updateEmitTraceTrail((String) allInfo.get("componentID"), info);
                Vector elements = new Vector();
                elements.add("traceID");
                elements.add("traceTrail");
                output(messageMake(elements));
            }
        }
    }

    public void boltExecute(BoltExecuteInfo info){
        if (allInfo.get("componentID") != "__system"){
            output("from boltExecute _trace: " + info.tuple.getValueByField("_trace"));
            if (info.tuple.getValueByField("_trace") != new HashMap() && info.tuple.getValueByField("_trace") != null){
                getBoltExecuteInfo(info);
                Vector elements = new Vector();
                elements.add("traceID");
                elements.add("fields");
                elements.add("values");
                output(messageMake(elements));
            }
        }
    }

//    public void spoutAck(SpoutAckInfo info){
//        if (allInfo.get("componentID") != "__system"){
//            output("from boltExecute _trace: " + info.tuple.getValueByField("_trace"));
//            if (info.tuple.getValueByField("_trace") != new HashMap() && info.tuple.getValueByField("_trace") != null){
//                getBoltExecuteInfo(info);
//                Vector elements = new Vector();
//                elements.add("traceID");
//                elements.add("fields");
//                elements.add("values");
//                output(messageMake(elements));
//            }
//        }
//    }
}

