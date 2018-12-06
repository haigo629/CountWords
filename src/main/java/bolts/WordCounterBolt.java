package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCounterBolt implements IRichBolt {

    Integer id;
    String name;
    Map counters;
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.counters = new HashMap();
        this.collector = collector;
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
    }

    @Override
    public void execute(Tuple input) {
        String str=input.getString(0);
        if(!counters.containsKey(str)){
            counters.put(str,1);
            System.out.println(str+": "+1);
        }else{
            Integer c = Integer.parseInt(counters.get(str).toString()) + 1;
            counters.put(str,c);
            System.out.println(str+": "+c);
        }
        collector.ack(input);
    }

    @Override
    /**
     * 集群关闭的时候显示单词数量
     */
    public void cleanup() {
        System.out.println("—-单词数["+name+"-"+id+"]--");
        for(Object key : counters.keySet()){
            System.out.println(key+": "+counters.get(key));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
