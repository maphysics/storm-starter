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

    private String logString = "\n";
    private String traceTrail;
    private String traceTrailNew;
    int traceinfoInt = -1;
    HashMap traceInfoNew = new HashMap();

    public void getBasicBoltExecuteInfo(BoltExecuteInfo info){
        allInfo.put("tuple", info.tuple);
        allInfo.put("trace", info.tuple.getBooleanByField("_trace"));
        allInfo.put("traceInfo", info.tuple.getValueByField("traceInfo"));
        HashMap traceInfo = (HashMap) allInfo.get("traceInfo");

        allInfo.put("traceId", traceInfo.get("traceId"));
        allInfo.put("traceTrail", traceInfo.get("traceTrail"));
    }

    public void getBoltExecuteInfo(BoltExecuteInfo info){
        allInfo.put("values", info.tuple.getValues());
    }

    public void getPrepareInfo(java.util.Map conf, TopologyContext context, String streamName){
        allInfo.put("componentID", context.getThisComponentId());
        if (allInfo.get("componentID") != "__acker"){
            allInfo.put("sources", context.getThisSources());
            allInfo.put("outputFields", context.getThisOutputFields(streamName));
            allInfo.put("targets", context.getThisTargets());
        }
    }

    public void messageMake(Vector<String> allInfoKeys){
        Iterator it = allInfo.keySet().iterator();
        while(it.hasNext()) {
            String key = (String) it.next();
            if (allInfoKeys.contains(key)){
                Object val = (Object) allInfo.get(key);
                logString = logString.concat(key + ": " + val + "\t");
            }
        }
    }

    public void output(Vector<String> allInfoKeys){
        Logger LOG = LoggerFactory.getLogger(TracerHook.class);
        messageMake(allInfoKeys);
        LOG.info(logString);
    }

    public void updateTraceTrail(String component){
        if ((Boolean) allInfo.get("trace") == true){
            Tuple tuple = (Tuple) allInfo.get("tuple");
            HashMap tInfo = (HashMap) tuple.getValueByField("traceInfo");
            String tTrail = (String) tInfo.get("traceTrail");
            tTrail = component + "(" + tTrail + ")";
            tInfo.put("traceTrail", tTrail);
            allInfo.put("traceTrail", tTrail);
            allInfo.put("newTraceInfo", tInfo);
        }
    }

    public void updateEmitTraceTrail(String component, EmitInfo info){
        Boolean trace = (Boolean) allInfo.get("trace");
        Tuple tuple = (Tuple) allInfo.get("tuple");

        System.out.println("trace: " + trace + " tuple:" + tuple);


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
    }

    public void emit(EmitInfo info){
        //May duplicate trail names, not sure
        updateEmitTraceTrail((String) allInfo.get("componentID"), info);
    }

    public void boltExecute(BoltExecuteInfo info){
        getBasicBoltExecuteInfo(info);
        getBoltExecuteInfo(info);
        updateTraceTrail((String) allInfo.get("componentID"));

        if ((Boolean) allInfo.get("trace") == true){
            Vector<String> allInfoKeys = new Vector();
            allInfoKeys.add("componentID");
            allInfoKeys.add("traceId");
            allInfoKeys.add("sources");
            allInfoKeys.add("outputFields");
            allInfoKeys.add("targets");
            allInfoKeys.add("traceTrail");
            allInfoKeys.add("values");
            output(allInfoKeys);
        }
    }
}

