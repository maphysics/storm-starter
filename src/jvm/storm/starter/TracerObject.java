package storm.starter;

/**
 * Created with IntelliJ IDEA.
 * User: maphysics
 * Date: 3/21/13
 * Time: 10:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class TracerObject {
    private int traceRecordNumber;
    private String tracePath;

    public TracerObject(int tRNum, String start){
        traceRecordNumber = tRNum;
        tracePath = start;
    }

    public void updatePath(String place){
        tracePath = place + "(" + tracePath + ")";
    }

    public String getTracePath(){
        return tracePath;
    }

    public int getTraceRecordNumber(){
        return traceRecordNumber;
    }
}
