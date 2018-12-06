package App;

import bolts.WordCounterBolt;
import bolts.WordNormalizerBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import spouts.WordReaderSpout;

/**
 * 主类
 * 在主类中创建拓扑和一个本地集群对象，
 * 以便于在本地测试和调试。
 * LocalCluster 可以通过 Config 对象，
 * 让你尝试不同的集群配置。
 * 比如，当使用不同数量的工作进程测试你的拓扑时，
 * 如果不小心使用了某个全局变量或类变量，你就能够发现错误
 */
public class App {
    public  static  void  main(String[] args)throws InterruptedException{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReaderSpout());
        builder.setBolt("word-normalizer", new WordNormalizerBolt()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounterBolt(),2).fieldsGrouping("word-normalizer", new Fields("word"));

        //配置
        Config conf = new Config();
        conf.put("wordsFile", "src/main/resources/words.txt");
        conf.setDebug(false);

        //运行拓扑
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
        Thread.sleep(1000);
        cluster.shutdown();
    }
}
