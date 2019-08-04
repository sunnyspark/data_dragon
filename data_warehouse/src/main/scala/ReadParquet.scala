import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadParquet {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder().appName("reader").master("local").getOrCreate()
		val dataFrame: DataFrame = spark.read.parquet("data_warehouse/data/tmp/par")
		dataFrame.show()
		spark.close()
	}
}
