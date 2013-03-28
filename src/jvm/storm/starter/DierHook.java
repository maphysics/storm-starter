package storm.starter;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: maphysics
 * Date: 3/21/13
 * Time: 2:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class DierHook extends BaseTaskHook {
    public static Logger LOG = LoggerFactory.getLogger(DierHook.class);

    private java.util.Map<GlobalStreamId,Grouping> sources;
    private String componentID;
    private Fields outputFields;
    private java.util.Map<java.lang.String,java.util.Map<java.lang.String,Grouping>> targets;
    private int _dieIndex;
    private int _sentenceIndex;

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
            _dieIndex = outputFields.fieldIndex("_die");
        }
        catch(Exception name){
            _dieIndex = -1;
        }

        try{
            _sentenceIndex = outputFields.fieldIndex("sentence");
        }
        catch(Exception name){
            _sentenceIndex = -1;
        }

        if (_dieIndex != -1 ){
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

            if (info.values.get(0) !=  new Values("startup").get(0) && info.values.size() > _dieIndex){
                //this checks that the tuple does has a bool at the correct index
                //metrcis tuple for instance don't
                //Also think there is a tuple sent out from the spout after running for awhile that messes with things
                try{
                    if (info.values.get(_dieIndex) instanceof Boolean){
                        Boolean die = (Boolean) info.values.get(_dieIndex);
                        //Only produce logString when input _trace == true
                        if (die == true){
                            String sentence = (String) info.values.get(_sentenceIndex);
                            throw new FailedException(sentence.concat(" is marked DIE!"));
                        }
                    }
                }
                catch(Exception name){
                    logString = "The tuple dies with the Exception: " + name + "\n" + logString;
                    logString = logString.concat("\n _dieIndex: " + _dieIndex);
                    logString = logString.concat("\n info.values.size(): " + info.values.size());
                    LOG.info(logString);
                }

            }
        }

    }
}


