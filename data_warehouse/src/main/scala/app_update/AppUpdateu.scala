package app_update

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import sparkutils.{LoggerUtil, SparkUtil}

object AppUpdateu {
	LoggerUtil.MyLogger

	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkUtil.getSparkSession("read")
		val dataFrame: Dataset[String] = spark.read.textFile("data_warehouse/data/tmp/person.txt")
		import spark.implicits._
		dataFrame.rdd.map(line => {
			val fields: Array[String] = line.split(",")
			val id: Int = fields(0).toInt
			val name: String = fields(1)
			val gender: String = fields(2)
			(id,name,gender)
		}).toDF("id","name","gender")
			.write.parquet("data_warehouse/data/tmp/par")
		spark.close()
	}
}
