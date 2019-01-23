package offlinead

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

//定义case class
case class AreaInfo(area_id: Int, area_name: String)

case class AdLogInfo(userid: Int, ip: String, clickTime: String, url: String, area_id: Int)


/**
  * project7 离线广告处理，分析PV
  */
object AdPVPro7 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().master("local").appName("AdPVPro7").getOrCreate()
    import spark.sqlContext.implicits._

    //创建地区表  "hdfs://hdp21:8020/input/project07/areainfo.txt"
    val areaInfoDF: DataFrame = spark.sparkContext.textFile("D:\\temp\\pro7\\areainfo.txt")
      .map(_.split(",")).map(x => new AreaInfo(x(0).toInt, x(1))).toDF()
    areaInfoDF.createOrReplaceTempView("areainfo")


    //创建广告点击日志表    "hdfs://hdp21:8020/flume/20180710/events-.1531169622734"
    val adClickInfoDF: DataFrame = spark.sparkContext.textFile("D:\\temp\\pro7\\userclicklog.txt" )
      .map(_.split(",")).map(x => new AdLogInfo(x(0).toInt, x(1), x(2), x(3), x(4).toInt)).toDF()
    adClickInfoDF.createOrReplaceTempView("adclickInfo")

    //定义SQL
    var sql = "select adclickinfo.url,areainfo.area_name,adclickinfo.clicktime,count(adclickinfo.clicktime) " +
      " from areainfo,adclickinfo " +
      " where areainfo.area_id=adclickinfo.area_id " +
      " group by adclickinfo.url,areainfo.area_name,adclickinfo.clicktime"

    //直接输出到屏幕
    spark.sql(sql).show()

    spark.stop()
  }

}
