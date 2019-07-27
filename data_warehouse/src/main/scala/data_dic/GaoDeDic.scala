package data_dic

import java.io.InputStream

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkutils.{LoggerUtil, SparkUtil}


object GaoDeDic {
	LoggerUtil.MyLogger

	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkUtil.getSparkSession("GaoDeDic")
		val noMatch: DataFrame = spark.read.text("data_warehouse/data/tmp/no_match_data")
		import spark.implicits._
		noMatch.map(line => {
			//逆地理编码
			val url = "https://restapi.amap.com/v3/geocode/regeo?"
			val key = "a21304e65185618370cbc2ae9be3a03d"


			//创建webapi
			val client: CloseableHttpClient = HttpClientBuilder.create().build()
			val get: HttpGet = new HttpGet(url + "key" + "=" + key + "&" + "location" +"="+ line)

			val response: CloseableHttpResponse = client.execute(get)
			val content: InputStream = response.getEntity.getContent
			import scala.collection.JavaConversions._
			val str: String = IOUtils.readLines(content).mkString("")

			val fields: Array[Double] = line.mkString("").split(",").map(_.toDouble)
			var geoHash: String = GeoHash.withCharacterPrecision(fields(1), fields(0), 6).toBase32
			//解析数据
			val json: JSONObject = JSON.parseObject(str)
			val status: String = json.getString("status")
			var province: String =""
			var city: String =""
			var district: String=""
			var bizName: String = ""
			if (status .equals("1") ) {
				val regeocode: JSONObject = json.getJSONObject("regeocode")
				val addressComponent: JSONObject = regeocode.getJSONObject("addressComponent")

				province= addressComponent.getString("province")
				city= addressComponent.getString("city")
				district= addressComponent.getString("district")

				val businessAreas: JSONArray = addressComponent.getJSONArray("businessAreas")
				if (businessAreas.size() > 0) {
					for (i <- 0 until businessAreas.size()) {
						val businessAreasJson= businessAreas.getJSONObject(i)
						val lng_lat: Array[Double] = businessAreasJson.getString("location").split(",").map(_.toDouble)
						bizName = businessAreasJson.getString("name")
						geoHash = GeoHash.withCharacterPrecision(lng_lat(1), lng_lat(0), 6).toBase32
						((geoHash, province, city, district), bizName)
					}
				}

			}
			((geoHash, province, city, district), bizName)
		})

		spark.close()
	}
}
