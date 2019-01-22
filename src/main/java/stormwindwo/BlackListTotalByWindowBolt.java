package stormwindwo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 每10秒钟，统计过去30秒内访问频率超过4次的用户信息。
 * 利用storm提供的窗口函数，进行处理统计
 */
public class BlackListTotalByWindowBolt extends BaseWindowedBolt {

    private OutputCollector collector;

    //定义一个集合，来保存该窗口处理的结果
    private Map<Integer, Integer> result = new HashMap<>();

    @Override
    //参数：inputWindow该窗口中的数据
    public void execute(TupleWindow tupleWindow) {
        //得到该窗口中的所有数据
        List<Tuple> tuples = tupleWindow.get();

        //处理该窗口中的所有数据
        for (Tuple tuple : tuples) {
            Integer userid = tuple.getIntegerByField("userid");
            Integer count = tuple.getIntegerByField("count");

            if (result.containsKey(userid)) {
                Integer integer = result.get(userid);
                result.put(userid, count + integer);
            } else {
                result.put(userid, count);
            }


            //输出
            System.out.println("统计的结果: " + result);

            //频率超过4次的用户信息  -----> 输出MySQL数据库
            if (result.get(userid) > 4) {
                //把这个用户信息发给下一个bolt组件 -----> 输出MySQL数据库
                this.collector.emit(new Values(userid, result.get(userid)));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userid", "PV"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
}
