package stormwindwo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

//第一步： 采集到kafka里面的原始数据进行处理
//并且统计 每个用户出现的次数
public class BlackListSplitBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //处理日志：1,201.105.101.102,http://mystore.jsp/?productid=1,2017020029,2,1
        String log = tuple.getString(0);

        //分词
        String[] words = log.split(",");

        //过滤掉，不满足要求的日志数据
        if (words.length == 6) {
            //每个user_id记一次数
            this.outputCollector.emit(new Values(Integer.parseInt(words[0]), 1));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 申明schema的格式: (user_id,1)
        outputFieldsDeclarer.declare(new Fields("userid", "count"));
    }
}
