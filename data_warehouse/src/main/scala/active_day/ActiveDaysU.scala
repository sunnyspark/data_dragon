package active_day

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import sparkutils.SparkUtil

import scala.collection.immutable

object ActiveDaysU {
	Logger.getLogger("org").setLevel(Level.WARN)

	def main(args: Array[String]): Unit = {
		val current_date = "2019-06-15"
		val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
		val data: DataFrame = spark.read.option("header", "true").csv("data_warehouse/data/appdata/active.csv")
		//导入隐式转化
		import spark.implicits._
		val userAndDate: Dataset[(String, (String, String))] = data.map(iter => {
			val user: String = iter.getAs[String](0)
			val start: String = iter.getAs[String](2)
			var end: String = iter.getAs[String](3)
			if (end == "9999-12-31") {
				end = current_date
			}
			(user, (start, end))
		})
		//转为rdd并按照key分组
		val userAndInterval: RDD[(String, List[(String, Long)])] = userAndDate.rdd.groupByKey().mapValues(iter => {
			val dataList: List[(String, String)] = iter.toList
			val startAndEnd: List[(Long, Long)] = dataList.map(tp => {
				val start = tp._1
				val end = tp._2
				val startTime: Long = new SimpleDateFormat("yyyy-MM-dd").parse(start).getTime / (1000 * 3600 * 24)
				val endTime: Long = new SimpleDateFormat("yyyy-MM-dd").parse(end).getTime / (1000 * 3600 * 24)
				(startTime, endTime)
			})

			//隔一天的天数
			val onedDays: List[(String, Long)] = startAndEnd.map(tp => {
				("隔1天", tp._2 - tp._1)
			})

			//隔n天的天数
			val ndays: immutable.IndexedSeq[(String, Long)] = for (i <- 0 until (startAndEnd.size - 1)) yield ("隔" + (startAndEnd(i + 1)._1 - startAndEnd(i)._2) + "天", 1L)
			ndays.toList ++ onedDays
		})
		val res: RDD[((String, String), Long)] = userAndInterval.flatMap(iter => {
			val user: String = iter._1
			iter._2.map(tp => {
				((user, tp._1), tp._2)
			})
		})
		//将结果聚合
		res.reduceByKey(_+_)
			.map(iter=>(iter._1._1,iter._1._2,iter._2))
			.toDF("user","interval","days")
			.show(20,false)
		spark.close()
	}
}
