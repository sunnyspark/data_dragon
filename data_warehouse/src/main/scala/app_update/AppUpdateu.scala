package app_update

import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkutils.{LoggerUtil, SparkUtil}

object AppUpdateu {
	LoggerUtil.MyLogger

	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkUtil.getSparkSession("appupdate")
		val dataFrame: DataFrame = spark.read.option("header", "true").csv("data_warehouse/data/appdata/app.csv")
		val lstRDD = dataFrame.rdd.map(iter => {
			val appName: String = iter.getAs[String](0)
			val appDate: String = iter.getAs[String](1)
			val appVer: String = iter.getAs[String](2)
			(appName,  appVer)
		}).groupByKey().mapValues(_.toSet)


		lstRDD.foreach(println)
	}
}
