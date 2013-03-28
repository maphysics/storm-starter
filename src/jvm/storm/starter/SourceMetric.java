package storm.starter;

import backtype.storm.metric.api.IMetric;

public class SourceMetric implements IMetric, java.io.Serializable {
    String _value = "";

    public SourceMetric() {
    }

    public void getSource(){
        _value = "The source here";
    }

    public Object getValueAndReset() {
        String ret = _value;
        _value = "";
        return ret;
    }
}
