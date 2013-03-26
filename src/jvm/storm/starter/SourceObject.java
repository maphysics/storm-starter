package storm.starter;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: maphysics
 * Date: 3/18/13
 * Time: 10:55 AM
 * To change this template use File | Settings | File Templates.
 */

/**
 * The idea behind this object is to implement a structure to store information for a record to be the tracing input
 */
public class SourceObject {
    Date timestamp;
    String source;
    Boolean _trace;

    public SourceObject(Date time, String sourcestring, Boolean trace){
        timestamp = time;
        source = sourcestring;
        _trace = trace;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public String getSource(){
        return source;
    }

    public Boolean get_trace(){
        return _trace;
    }

    public void setTimestamp(Date time){
        timestamp = time;
    }

    public void setSource(String s){
        source = s;
    }

    public void set_trace(boolean trace){
        _trace = trace;
    }
}
