package dic

import ch.hsr.geohash.GeoHash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sparkutils.SparkUtil

object BizParse {
	Logger.getLogger("org").setLevel(Level.WARN)

	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkUtil.getSparkSession("BizParse")
		val dataRDD: RDD[String] = spark.sparkContext.textFile("F:\\WorkPlace\\data_dragon\\data_warehouse\\data\\biz_orgin\\商圈数据.sql")
		import spark.implicits._
		dataRDD.map(line => {
			val fields: Array[String] = line.split(", ")
			val province: String = fields(1).substring(1,fields(1).length-1)
			val city: String = fields(2).substring(1,fields(2).length-1)
			val district: String = fields(3).substring(1,fields(3).length-1)
			val biz: String = fields(4).substring(1,fields(4).length-1)
			val lgn: String = fields(5).substring(1,fields(5).length-1)
			val lat: String = fields(6).substring(1,fields(6).length-3)
			val geoHash: String = GeoHash.withCharacterPrecision(lat.toDouble,lgn.toDouble,6).toBase32
			(geoHash,province,city,district,biz)
		}).toDF("geoHash","province","city","district","biz")
    		.createTempView("biz")

		val url = "jdbc:mysql://localhost:3306/doit08?charaterEncoding=utf-8"
		spark.sql("select * from biz").repartition(1).write.parquet("data_warehouse/data/biz_dic")
		spark.close()
	}
}
