package app_update

import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkutils.{LoggerUtil, SparkUtil}

object AppUpdate {
	LoggerUtil.MyLogger
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkUtil.getSparkSession("AppUpdate")
		val appDF: DataFrame = spark.read.option("header","true").csv("data_warehouse/data/appdata/app.csv")
		val dataFrame: DataFrame = appDF.toDF("app_name","app_date","app_ver")
		dataFrame.createTempView("app")
		spark.sql(
			"""
			  |with tmp as(select app_name,app_date,app_ver from app group by app_name,app_date,app_ver)
			  |select app_name,app_date,app_ver,app_ver2 from
			  |(select app_name,app_date,app_ver,
			  |lead(app_ver) over(partition by app_name order by app_ver) app_ver2
			  |from tmp) o
			  |where o.app_ver2 is not null
			""".stripMargin).show()
		spark.close()
	}
}
