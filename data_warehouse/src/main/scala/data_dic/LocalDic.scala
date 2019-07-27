package data_dic

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import sparkutils.{LoggerUtil, SparkUtil}

object LocalDic {
	LoggerUtil.MyLogger

	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkUtil.getSparkSession("localDic")
		val localDS: Dataset[String] = spark.read.textFile("data_warehouse/data/area_orgin/省市区GPS坐标数据.sql")
		import spark.implicits._
		localDS.map(line => {
			//INSERT INTO `t_md_areas` VALUES (620502, '秦州区', 620500, '', 3, 1, 105.7202342335681, 34.58219866690643, 105.72417, 34.58088, 105.58117092708984, 34.344448280621755);
			val fields: Array[String] = line.split(", ")
			val childer_code: String = fields(0).substring(fields(0).length - 6, fields(0).length)
			val name: String = fields(1).substring(1, fields(1).length - 1)
			val parent_code: String = fields(2)
			val level: String = fields(4)
			val lat: String = fields(fields.length - 1).substring(0, fields(fields.length - 1).length - 2)
			val lng: String = fields(fields.length - 2)

			(childer_code, name, parent_code, level, lng, lat)
		}).toDF("childer_code", "name", "parent_code", "level", "lng", "lat")
			.createTempView("location")

		val localDic: DataFrame = spark.sql(
			"""
			  |select a.name distract,b.name city,c.name province,a.lng,a.lat
			  |from location a join location b
			  |on a.level = 3 and a.parent_code = b.childer_code
			  |join location c
			  |on b.parent_code = c.childer_code and b.level = 2
			""".stripMargin)
		localDic.map(row => {
			val province: String = row.getAs[String]("province")
			val city: String = row.getAs[String]("city")
			val distract: String = row.getAs[String]("distract")
			var lng: Double = 0
			var lat: Double = 0
			try {
				lng = row.getAs[String]("lng").toDouble
				lat = row.getAs[String]("lat").toDouble
			} catch {
				case e: NumberFormatException =>
			}
			val localDic: String = GeoHash.withCharacterPrecision(lat, lng, 5).toBase32
			(localDic, province, city, distract)
		}).coalesce(1).write.mode("overwrite").parquet("data_warehouse/data/tmp/area_dic")

		spark.close()
	}
}
