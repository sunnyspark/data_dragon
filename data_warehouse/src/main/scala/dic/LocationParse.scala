package dic

import java.util.Properties

import ch.hsr.geohash.GeoHash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkutils.SparkUtil

object LocationParse {
	Logger.getLogger("org").setLevel(Level.WARN)

	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkUtil.getSparkSession("location")
		val url = "jdbc:mysql://localhost:3306/doit08"
		val props: Properties = new Properties()
		props.setProperty("user", "root")
		props.setProperty("password", "123456")
		val locationDF: DataFrame = spark.read.jdbc(url, "t_md_location", props)
		import spark.implicits._
		locationDF.map(row => {
			val province: String = row.getAs[String]("province")
			val city: String = row.getAs[String]("city")
			val distract: String = row.getAs[String]("distract")
			val lng = row.getAs[Double]("lng")
			val lat = row.getAs[Double]("lat")
			val geoHash: String = GeoHash.withCharacterPrecision(lat, lng, 5).toBase32
			(geoHash, province, city, distract)
		}).createTempView("location")
		spark.sql(
			"""
			  |select _1 geoHash,_2 province,_3 city,_4 distract from location
			""".stripMargin).coalesce(1).write.mode("overwrite").parquet("data_warehouse/data/area_dic")
		spark.close()
	}
}
