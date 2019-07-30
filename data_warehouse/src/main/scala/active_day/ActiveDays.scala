package active_day

import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkutils.{LoggerUtil, SparkUtil}

import scala.collection.immutable

object ActiveDays {
	LoggerUtil.MyLogger

	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkUtil.getSparkSession("ActiveDays")
		val current_day = "2019-06-14"

		//读取文件
		val data: DataFrame = spark.read.option("header", "true").csv("data_warehouse/data/appdata/active.csv")
		import spark.implicits._
		/*
		|  a|2019-06-01|2019-06-01|2019-06-04|
		|  a|2019-06-01|2019-06-06|2019-06-06|
		|  a|2019-06-01|2019-06-08|2019-06-08|
		|  a|2019-06-01|2019-06-10|9999-12-31|
		 */
		val midRes: RDD[(String, Long)] = data.rdd.map(iter => {
			val user: String = iter.getAs[String](0)
			val start: String = iter.getAs[String](2)
			var end: String = iter.getAs[String](3)
			if (end == "9999-12-31") {
				end = current_day
			}
			(user, (start, end))
		})
			.groupByKey()
			.mapValues(iter => {
				val qjLst = iter.toList

				val qjListEporch: List[(Long, Long)] = qjLst
					.map(tp => {

						val startArr = tp._1.split("-")
						val endArr = tp._2.split("-")
						val start: Long = LocalDate.of(startArr(0).toInt, startArr(1).toInt, startArr(2).toInt).toEpochDay
						val end: Long = LocalDate.of(endArr(0).toInt, endArr(1).toInt, endArr(2).toInt).toEpochDay
						(start, end)
					})
				//生成隔一天的value
				val oneDay: List[(String, Long)] = qjListEporch.map(tp => {
					("隔1天", tp._2 - tp._1)
				})

				//生成隔n天的value
				val ndays: immutable.IndexedSeq[(String, Long)] = for (i <- 0 until (qjListEporch.size - 1)) yield ("隔" + (qjListEporch(i + 1)._1 - qjListEporch(i)._2) + "天", 1L)
				oneDay ++ ndays.toList
			})
			//将value值扁平化
			.flatMap(_._2)
			.reduceByKey(_ + _)
		midRes.toDF("intervals", "days").show(20, false)


		spark.close()
	}
}
