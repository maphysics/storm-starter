package storm.starter;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.*;
import backtype.storm.task.TopologyContext;

import java.util.HashMap;

public class BaseTracerHook extends BaseTaskHook {
    public void getExtraPrepareInfo(java.util.Map conf, TopologyContext context, String streamName){}
    //    public void getExtraBoltExecuteInfo(BoltExecuteInfo info){}
    public void getExtraEmitInfo(EmitInfo info){}
    public void getExtraBoltAckInfo(BoltAckInfo info){}
    public void getExtraBoltFailInfo(BoltFailInfo info){}
    public void getExtraSpoutAckInfo(SpoutAckInfo info){}
    public void getExtraSpoutFailInfo(SpoutFailInfo info){}

    public void prepareOutput(){}
    public void boltAckOutput(){}
    public void boltFailOutput(){}
    public void boltExecuteOutput(){}
    public void spoutAckOutput(){}
    public void spoutFailOutput(){}
    public void emitOutput(){}

    public void output(String basicString){}
    public String messageMake(HashMap allInfo, String step, String... allInfoKeys){
        String component = (String) allInfo.get("componentID");
        String logString = "from " + step + " step of componentID: " + component + ": ";
        for(int i=0; i<allInfoKeys.length; i++){
            if(allInfo.containsKey(allInfoKeys[i])){
                Object val = allInfo.get(allInfoKeys[i]);
                logString = logString.concat(allInfoKeys[i] + ": " + val + "\t");
            }
        }
        logString.concat("\n");
        return logString;
    }

    public final HashMap getSpoutAckInfo(SpoutAckInfo info){
        HashMap spoutAckInfo = new HashMap();
        spoutAckInfo.put("completeLatencyMs", info.completeLatencyMs);
        return spoutAckInfo;
    }

    public final HashMap getSpoutFailInfo(SpoutFailInfo info){
        HashMap spoutFailInfo = new HashMap();
        spoutFailInfo.put("failLatencyMs", info.failLatencyMs);
        return spoutFailInfo;
    }

    public final HashMap getBoltAckInfo(BoltAckInfo info){
        HashMap boltAckInfo = new HashMap();
        boltAckInfo.put("ackingTaskID", info.ackingTaskId);
        boltAckInfo.put("processLatencyMs", info.processLatencyMs);
        return boltAckInfo;
    }

    public final HashMap getBoltFailInfo(BoltFailInfo info){
        HashMap boltFailInfo = new HashMap();
        boltFailInfo.put("failingTaskID", info.failingTaskId);
        boltFailInfo.put("failLatencyMs", info.failLatencyMs);
        return boltFailInfo;
    }

//    public final HashMap getBasicBoltExecuteInfo(BoltExecuteInfo info){
//        HashMap basicBoltExecuteInfo = new HashMap();
//        basicBoltExecuteInfo.put("tuple", info.tuple);
//        basicBoltExecuteInfo.put("trace", info.tuple.getValueByField("_trace"));
//        basicBoltExecuteInfo.put("executeLatencyMs", info.executeLatencyMs);
//        HashMap trace = (HashMap) basicBoltExecuteInfo.get("trace");
//        basicBoltExecuteInfo.put("traceID", trace.get("traceID"));
//        basicBoltExecuteInfo.put("traceMessage", trace.get("traceMessage"));
//        return basicBoltExecuteInfo;
//    }

//    public final HashMap getBoltExecuteInfo(BoltExecuteInfo info){
//        HashMap boltExecuteInfo = getBasicBoltExecuteInfo(info);
//        boltExecuteInfo.put("fields", info.tuple.getFields());
//        boltExecuteInfo.put("values", info.tuple.getValues());
//        return boltExecuteInfo;
//    }

    public final HashMap getPrepareInfo(java.util.Map conf, TopologyContext context, String streamName){
        HashMap prepareInfo = new HashMap();
        prepareInfo.put("componentID", context.getThisComponentId());
        if (prepareInfo.get("componentID") != "__acker"){
            prepareInfo.put("streams", context.getThisStreams());
            prepareInfo.put("sources", context.getThisSources());
            prepareInfo.put("outputFields", context.getThisOutputFields(streamName));
            prepareInfo.put("targets", context.getThisTargets());
        }
        else{
            prepareInfo.put("sources", "from __acker");
            prepareInfo.put("outputfields", "from __acker");
            prepareInfo.put("targets", "from __acker");
        }
        return prepareInfo;
    }
}
