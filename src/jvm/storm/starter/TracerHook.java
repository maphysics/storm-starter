package storm.starter;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.task.TopologyContext;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.tuple.Fields;
import backtype.storm.generated.Bolt;
import backtype.storm.tuple.Values;
import com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;


/**
 * Created with IntelliJ IDEA.
 * User: maphysics
 * Date: 3/20/13
 * Time: 12:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class TracerHook extends BaseTaskHook {
    public static Logger LOG = LoggerFactory.getLogger(TracerHook.class);

    private java.util.Map<GlobalStreamId,Grouping> sources;
    private String componentID;
    private Fields outputFields;
    private java.util.Map<java.lang.String,java.util.Map<java.lang.String,Grouping>> targets;
    private int _traceIndex;
    private String logString = "";
    private HashMap _traceInfo = new HashMap();
    private int traceinfoInt = 0;

    @Override
    public void prepare(java.util.Map conf, TopologyContext context){
        componentID = context.getThisComponentId();
        targets = context.getThisTargets();
        if (componentID != "__acker"){
            sources = context.getThisSources();
            outputFields = context.getThisOutputFields("default");
            targets = context.getThisTargets();
        }
    }

    @Override
    public void emit(EmitInfo info){
        try{
            //Get the location of _trace in the output fields, this assumes it was the same place in the input fields
            _traceIndex = outputFields.fieldIndex("_trace");
        }
        catch(Exception name){
            _traceIndex = -1;
        }
        try{
            traceinfoInt = outputFields.fieldIndex("traceInfo");
            _traceInfo = (HashMap) info.values.get(traceinfoInt);
        }
        catch(Exception name){
            traceinfoInt = -1;
            _traceInfo = new HashMap();
        }

        if (_traceIndex != -1 ){
            String logString = "\n componentID   sources   outputFields   targets   outputValues \n ";


            logString = logString.concat(componentID + " \t ");
            if (componentID != "__acker"){
                logString = logString.concat(sources + " \t ");
                logString = logString.concat(outputFields + " \t ");
                logString = logString.concat(targets + " \t ");
            }

            //__system is a system stream emitted by bolts and spouts and it only emits startup events
            //ignore these as they don't have the format I want
            //FIXME! figure out how to handle _trace fields not being where I think they are
            logString = logString.concat(info.values + " \t ");

            if (info.values.get(0) !=  new Values("startup").get(0) && info.values.size() > _traceIndex){
                //this checks that the tuple does has a bool at the correct index
                //metrcis tuple for instance don't
                //Also think there is a tuple sent out from the spout after running for awhile that messes with things
                try{
                    if (info.values.get(_traceIndex) instanceof Boolean){
                        Boolean fail = (Boolean) info.values.get(_traceIndex);
                        //Only produce logString when input _trace == true
                        if (fail == true){
                            LOG.info(logString);                    }
                    }
                }
                catch(Exception name){
                    logString = "The tuple dies with the Exception: " + name + "\n" + logString;
                    logString = logString.concat("\n _traceIndex: " + _traceIndex);
                    logString = logString.concat("\n info.values.size(): " + info.values.size());
                    LOG.info(logString);
                }
//        try{
//            //Get the location of _trace in the output fields, this assumes it was the same place in the input fields
//            _traceIndex = outputFields.fieldIndex("_trace");
//            Boolean trace = (Boolean) info.values.get(_traceIndex);
//        }
//        catch(Exception name){
//            _traceIndex = -1;
//        }
////        try{
////            traceinfoInt = outputFields.fieldIndex("traceInfo");
////            _traceInfo = (HashMap) info.values.get(traceinfoInt);
////        }
////        catch(Exception name){
////            traceinfoInt = -1;
////            _traceInfo = new HashMap();
////        }
//        if (_traceIndex != -1 ){
//
//            logString = logString.concat("\ncomponentID: \t" + componentID + " \t ");
////            if (componentID != "__acker"){
////                logString = logString.concat("\nsources: \t" + sources + " \t ");
////                logString = logString.concat("\noutputFields: \t" +outputFields + " \t ");
////                logString = logString.concat("\ntargets: \t" + targets + " \t ");
////            }
//
//            //__system is a system stream emitted by bolts and spouts and it only emits startup events
//            //ignore these as they don't have the format I want
//            //FIXME! figure out how to handle _trace fields not being where I think they are
//
//
//            if (info.values.get(0) !=  new Values("startup").get(0)){
//                //this checks that the tuple does has a bool at the correct index
//                //metrcis tuple for instance don't
//                //Also think there is a tuple sent out from the spout after running for awhile that messes with things
//
//                if (info.values.get(_traceIndex) instanceof Boolean){
//                    Boolean trace = (Boolean) info.values.get(_traceIndex);
//                    //Only produce logString when input _trace == true
//                    if (trace == true){
////                        String _traceTrail = (String) _traceInfo.get("traceTrail");
////                        _traceTrail = componentID + "(" + _traceTrail + ")";
////                        _traceInfo.put("traceTrail", _traceTrail);
////                        if (traceinfoInt != -1){
////                            info.values.remove(traceinfoInt);
////                            info.values.add(traceinfoInt, _traceInfo);
////                            logString = logString.concat("\nupdateTrail: \t Yes");
////                        }
////                        else{
////                            logString = logString.concat("\nupdateTrail: \t No");
////                        }
//                        logString = logString.concat("\nvalues: \t" + info.values + " \t ");
//                        LOG.info(logString);
//                    }
//                }
//            }
            }
        }
    }
}

