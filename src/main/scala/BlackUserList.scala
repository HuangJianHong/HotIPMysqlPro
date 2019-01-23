import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

//定义两个case class类
case class HotIP(user_id: Int, pv: Int)

//只用到user_id 和 user_name;
case class UserInfo(user_id: Int, username: String)

/**
  * DStreaming 实时流窗口函数， 处理kafka中缓存的数据 SparkStreaming
  * 并用SparkSQL进行多表查询处理
  */
object BlackUserList {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //创建SparkContext 和 StreamingContext
    val conf: SparkConf = new SparkConf().setAppName("BlackUserList").setMaster("local[2]")
    val sc = new SparkContext(conf)

    /**
      * 使用以及存在的SparkContext创建 StreamingContext
      * 否则一个scala程序存在多个SparkContext会异常FC
      */
    val ssc = new StreamingContext(sc, Seconds(10))

    //由于使用Spark SQL分析数据，创建SQLContext
    val sqlContext = new SQLContext(ssc.sparkContext)
    import sqlContext.implicits._

    //从HDFS上加载用户信息
    val userInfo: DataFrame = sc.textFile("hdfs://hdp21:8020/input/userinfo.txt")
      .map(_.split(","))
      .map(x => new UserInfo(x(0).toInt, x(1))).toDF
    userInfo.createOrReplaceTempView("userinfo")

    //从kafka中接收点击日志，分析用户的PV
    val topics: Map[String, Int] = Map("mytopic" -> 1)

    //创建Kafka的输入流，只能使用基于Receiver的方式
    val kafkaSteam: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, "192.168.18.21:2181", "mygroup", topics)

    //日志信息：1,201.105.101.102,http://mystore.jsp/?productid=1,2017020020,1,1
    //key值是null, 我们取第二个元素就是日志信息
    val logRDD: DStream[String] = kafkaSteam.map(_._2)

    /**
      * 统计用户的PV: 使用窗口函数操作
      */
    val hot_user_id = logRDD.map(_.split(",")).map(x => (x(0), 1)) //每个用户的ID，记一次数
      .reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(30), Seconds(10)) //窗口的长度           滑动距离

    /**
      * 得到这段时间内的黑名单
      */
    val result: DStream[(String, Int)] = hot_user_id.filter(x => x._2 > 4)

    //查询黑名单用户的信息: DStream:RDD流
    result.foreachRDD(rdd => {
      val hotUserTable: DataFrame = rdd.map(x => new HotIP(x._1.toInt, x._2)).toDF()
      hotUserTable.createOrReplaceTempView("hotip")

      //关联用户表，查询黑名单的信息
      val sqlString = "select userinfo.user_id,userinfo.username,hotip.pv from userinfo,hotip where userinfo.user_id=hotip.user_id"
      sqlContext.sql(sqlString).show()
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
