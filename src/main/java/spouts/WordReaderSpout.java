package spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

/**
 * 按行读取文件并分发给每个元组
 */
public class WordReaderSpout implements IRichSpout {

    private SpoutOutputCollector collector;

    private FileReader fileReader = null;

    private boolean completed = false;

    private TopologyContext context;

    public  boolean isDistributed(){
        return false;
    }

    @Override
    /**
     * 创建一个文件并维持一个collectord对象
     * 第一个被调用的方法
     * conf 配置对象
     * context 包含所有拓扑数据
     * collector 它能让我们发布交给 bolts 处理的数据。
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try{
            this.context = context;
            System.out.println(conf.get("wordsFile").toString());
            this.fileReader = new FileReader(conf.get("wordsFile").toString());

        }catch (Exception e){
            throw  new RuntimeException("Error reading file ["+conf.get("wordsFile")+"]");
        }
        this.collector = collector;

    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    /**
     * 分发文件中的文本行，这个方法会被不断调用，直到整个文件读完了，我们将等待并返回
     *
     * 通过这个方法向bolts发送待处理的数据
     */
    public void nextTuple() {
        if(completed){
            try{
                Thread.sleep(1);
            }catch (InterruptedException e){

            }
            return;
        }
        String str;
        //创建reader
        BufferedReader reader = new BufferedReader(fileReader);
        try{
            //读取所有文本行
            while((str = reader.readLine()) != null){
                //按行发布一个新值
                this.collector.emit(new Values(str),str);
            }
        }catch (Exception e){
            throw new RuntimeException("Error reading tuple",e);
        }finally {
            completed = true;
        }
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("OK"+msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("FAIL:"+msgId);
    }

    @Override
    /**
     * 声明输入域“word”
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
