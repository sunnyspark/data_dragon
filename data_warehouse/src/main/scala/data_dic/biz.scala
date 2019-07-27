package data_dic

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.{Dataset, SparkSession}
import sparkutils.{LoggerUtil, SparkUtil}

object biz {
	LoggerUtil.MyLogger

	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkUtil.getSparkSession("biz")
		val bizDS: Dataset[String] = spark.read.textFile("data_warehouse/data/biz_orgin/商圈数据.sql")
		import spark.implicits._
		bizDS.map(iter => {
			//('1', '北京市', '北京市', '东城区', '王府井', '116.412987', '39.908416');
			val fields: Array[String] = iter.split(", ")
			val province: String = fields(1).substring(1, fields(1).length - 1)
			val city: String = fields(2).substring(1, fields(2).length - 1)
			val distract: String = fields(3).substring(1, fields(3).length - 1)
			val biz: String = fields(4).substring(1, fields(4).length - 1)
			var bizGeo: String = null
			try {
				val lng = fields(5).substring(1, fields(5).length - 1).toDouble
				val lat = fields(6).substring(1, fields(6).length - 3).toDouble
				bizGeo= GeoHash.withCharacterPrecision(lat,lng,6).toBase32
			} catch {
				case e:Exception =>
			}
			(bizGeo, province, city, distract, biz)
		}).coalesce(1).write.mode("overwrite").parquet("data_warehouse/data/tmp/biz_dic")
		spark.close()
	}
}
