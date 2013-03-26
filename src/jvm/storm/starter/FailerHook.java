package storm.starter;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: maphysics
 * Date: 3/21/13
 * Time: 1:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class FailerHook extends BaseTaskHook {
    public static Logger LOG = LoggerFactory.getLogger(FailerHook.class);

    private java.util.Map<GlobalStreamId,Grouping> sources;
    private String componentID;
    private Fields outputFields;
    private java.util.Map<java.lang.String,java.util.Map<java.lang.String,Grouping>> targets;
    private int _failIndex;

    @Override
    public void prepare(java.util.Map conf, TopologyContext context){
        componentID = context.getThisComponentId();
        targets = context.getThisTargets();;
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
            _failIndex = outputFields.fieldIndex("_fail");
        }
        catch(Exception name){
            _failIndex = -1;
        }
        if (_failIndex != -1 ){
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

            if (info.values.get(0) !=  new Values("startup").get(0) && info.values.size() > _failIndex){
                //this checks that the tuple does has a bool at the correct index
                //metrcis tuple for instance don't
                //Also think there is a tuple sent out from the spout after running for awhile that messes with things
                try{
                    if (info.values.get(_failIndex) instanceof Boolean){
                        Boolean fail = (Boolean) info.values.get(_failIndex);
                        //Only produce logString when input _trace == true
                        if (fail == true){
                            LOG.info(logString);                    }
                    }
                }
                catch(Exception name){
                    logString = "The tuple dies with the Exception: " + name + "\n" + logString;
                    logString = logString.concat("\n _failIndex: " + _failIndex);
                    logString = logString.concat("\n info.values.size(): " + info.values.size());
                    LOG.info(logString);
                }

            }
        }

    }
}

