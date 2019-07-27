package datapreprocess

import java.util

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import sparkutils.SparkUtil

object LogPre {
	Logger.getLogger("org").setLevel(Level.WARN)

	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkUtil.getSparkSession("LogPre")
		val logDS: Dataset[String] = spark.read.textFile(
			"data_warehouse/data/logs/2019-06-15/15.doit.mall.access.log.gz"
			/*"data_warehouse/data/logs/2019-06-16/16.doit.mall.access.log",
			"data_warehouse/data/logs/2019-06-17/doit.mall.access.log",
			"data_warehouse/data/logs/2019-06-18/doit.mall.access.log",
			"data_warehouse/data/logs/2019-06-19/19.doit.mall.access.log"*/
		)
		import spark.implicits._
		val logBean: Dataset[LogBean] = logDS.map(line => {
			var bean: LogBean = null
			try {
				val json: String = line.split(" --> ")(1)
				val jsonObject: JSONObject = JSON.parseObject(json)
				val uObject: JSONObject = jsonObject.getJSONObject("u")
				val cookieid: String = uObject.getString("cookieid")
				val account: String = uObject.getString("account")

				val phoneObject: JSONObject = uObject.getJSONObject("phone")
				val imei = phoneObject.getString("imei")
				val osName = phoneObject.getString("osName")
				val osVer = phoneObject.getString("osVer")
				val resolution = phoneObject.getString("resolution")
				val androidId = phoneObject.getString("androidId")
				val manufacture = phoneObject.getString("manufacture")
				val deviceId = phoneObject.getString("deviceId")

				val appObject: JSONObject = uObject.getJSONObject("app")
				val appid = appObject.getString("appid")
				val appVer = appObject.getString("appVer")
				val release_ch = appObject.getString("release_ch")
				val promotion_ch = appObject.getString("promotion_ch")

				val locObject: JSONObject = uObject.getJSONObject("loc")
				val areacode = locObject.getString("areacode")
				val longtitude = locObject.getDouble("longtitude")
				val latitude = locObject.getDouble("latitude")
				val carrier = locObject.getString("carrier")
				val netType = locObject.getString("netType")


				val sessionId = uObject.getString("sessionId")
				val logType = jsonObject.getString("logType")
				val commit_time = jsonObject.getLong("commit_time")

				val event: JSONObject = jsonObject.getJSONObject("event")
				val keys: util.Set[String] = event.keySet()
				import scala.collection.JavaConversions._
				val eventMap: Map[String, String] = keys.map(key => (key, event.getString(key))).toMap

				bean = LogBean(
					cookieid,
					account,
					imei,
					osName,
					osVer,
					resolution,
					androidId,
					manufacture,
					deviceId,
					appid,
					appVer,
					release_ch,
					promotion_ch,
					areacode,
					longtitude,
					latitude,
					carrier,
					netType,
					sessionId,
					logType,
					commit_time,
					eventMap
				)
			} catch {
				case e: Exception => //e.printStackTrace()
			}
			bean
		})
		//			logBean.show(100)
		//过滤后的数据
		val filterData: Dataset[LogBean] = logBean.filter(bean => {
			val account: String = bean.account
			val cookieid: String = bean.cookieid
			val androidId: String = bean.androidId
			val deviceId: String = bean.deviceId
			val sessionId: String = bean.sessionId
			val imei: String = bean.imei
			val builder: StringBuilder = new StringBuilder()
			val stringBuilder: StringBuilder = builder.append(account).append(cookieid).append(androidId).append(deviceId).append(sessionId).append(imei)
			val str: String = stringBuilder.replaceAllLiterally("null", "")
			StringUtils.isNotBlank(str)
		})
		//		filterData.show()
		//读取地理位置和商业圈字典
		val areaDic: DataFrame = spark.read.parquet("data_warehouse/data/area_dic")
		//将搜集到Driver端
		val areaMap: Map[String, (String, String, String)] = areaDic.map(iter => {
			val geoHash: String = iter.getAs[String]("geoHash")
			val province: String = iter.getAs[String]("province")
			val city: String = iter.getAs[String]("city")
			val distract: String = iter.getAs[String]("distract")
			(geoHash, (province, city, distract))
		}).collect().toMap
		//将其广播
		val areaBD: Broadcast[Map[String, (String, String, String)]] = spark.sparkContext.broadcast(areaMap)

		//将商业圈收到Driver端
		val bizDic: DataFrame = spark.read.parquet("data_warehouse/data/biz_dic")
		val bizMap: Map[String, (String, String, String, String)] = bizDic.map(iter => {
			val geoHash: String = iter.getAs[String]("geoHash")
			val province: String = iter.getAs[String]("province")
			val city: String = iter.getAs[String]("city")
			val district: String = iter.getAs[String]("district")
			val biz: String = iter.getAs[String]("biz")
			(geoHash, (province, city, district, biz))
		}).collect().toMap
		val bizBD: Broadcast[Map[String, (String, String, String, String)]] = spark.sparkContext.broadcast(bizMap)

		//集成地理位置
		val locationData: Dataset[LogBean] = filterData.map(bean => {
			val lat: Double = bean.latitude
			val lng: Double = bean.longtitude
			val geoHash2Biz: String = GeoHash.withCharacterPrecision(lat, lng, 6).toBase32
			val geoHash2Area: String = GeoHash.withCharacterPrecision(lat, lng, 5).toBase32

			//获取广播的变量
			val bizValue: Map[String, (String, String, String, String)] = bizBD.value
			val areaValue: Map[String, (String, String, String)] = areaBD.value

			val bizInfo: (String, String, String, String) = bizValue.getOrElse(geoHash2Biz, ("", "", "", ""))
			bean.province = bizInfo._1
			bean.city = bizInfo._2
			bean.district = bizInfo._3
			bean.biz = bizInfo._4

			if (bizInfo._1.equals("")) {
				val areaInfo: (String, String, String) = areaValue.getOrElse(geoHash2Area, ("", "", ""))
				bean.province = areaInfo._1
				bean.city = areaInfo._2
				bean.district = areaInfo._3
			}
			bean
		})
		locationData.filter(bean => StringUtils.isBlank(bean.province))
			.map(bean => (bean.latitude + "," + bean.longtitude))
			.toDF()
    		.coalesce(1)
			.write
			.mode(SaveMode.Overwrite)
			.text("data_warehouse/data/no_match_data")

		//标题分词
		val result: Dataset[LogBean] = locationData.map(bean => {
			val option= bean.event.get("title")
			if (option != None) {
				val terms: util.List[Term] = HanLP.segment(option.get)
				//TODO 停止词过滤
				import scala.collection.JavaConversions._
				val keyword: String = terms.map(term => term.word).filter(_.size > 1).mkString(" ")
				bean.event.+=("keyword" -> keyword)
			}
			bean
		})
		result.show(1000, false)
		spark.close()
	}
}
