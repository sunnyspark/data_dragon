import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object WordCount {
	Logger.getLogger("org").setLevel(Level.WARN)
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local").getOrCreate()
		import spark.implicits._
		val dataSet: Dataset[String] = spark.createDataset(Seq("a,b,c", "a,e,a,c", "a,e,c,b"))
		val res: DataFrame = dataSet.flatMap((_.split(","))).map((_, 1)).toDF("word","num")
		res.createTempView("word_view")
		spark.sql(
			"""
			  |select word,count(1) from word_view group by word order by word
			""".stripMargin).show(10, false)

		//转为rdd在进行reduceByKey
		dataSet.rdd.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _).sortByKey().take(10).foreach(println)

		spark.close()
	}
}
