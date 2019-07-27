package data_process

import java.{lang, util}

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import sparkutils.{LoggerUtil, SparkUtil}

object LogProcess {
	LoggerUtil.MyLogger

	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkUtil.getSparkSession("LogProcess")
		val logDS: Dataset[String] = spark.read.textFile("data_warehouse/data/logs/2019-06-15")
		import spark.implicits._
		val logBeanDS: Dataset[LogBean] = logDS.map(iter => {
			var bean: LogBean = null
			try {
				val json: String = iter.split(" --> ")(1)
				val objectJson: JSONObject = JSON.parseObject(json)

				val uObject: JSONObject = objectJson.getJSONObject("u")
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
				val appid: String = appObject.getString("appid")
				val appVer: String = appObject.getString("appVer")
				val release_ch: String = appObject.getString("release_ch")
				val promotion_ch: String = appObject.getString("promotion_ch")

				val locObject: JSONObject = uObject.getJSONObject("loc")
				val areacode: String = locObject.getString("areacode")
				val longtitude: Double = locObject.getDouble("longtitude")
				val latitude: Double = locObject.getDouble("latitude")
				val carrier: String = locObject.getString("carrier")
				val netType: String = locObject.getString("netType")

				val sessionId: String = uObject.getString("sessionId")

				val logType: String = objectJson.getString("logType")
				val commit_time: Long = objectJson.getLong("commit_time")
				val eventObject: JSONObject = objectJson.getJSONObject("event")
				val keys: util.Set[String] = eventObject.keySet()
				import scala.collection.JavaConversions._
				val eventMap: Map[String, String] = keys.map(key => (key, eventObject.getString(key))) toMap

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
					commit_time: Long,
					eventMap
				)
			} catch {
				case e: Exception =>
			}
			bean
		})

		//过滤 account | cookieid | sessionid | imei |deviceId |androidId全为空的记录！
		val filtered: Dataset[LogBean] = logBeanDS.filter(bean => {
			val account: String = bean.account
			val cookieid: String = bean.cookieid
			val sessionId: String = bean.sessionId
			val imei: String = bean.imei
			val deviceId: String = bean.deviceId
			val androidId: String = bean.androidId

			val builder: StringBuilder = new StringBuilder()
			val stringBuilder: StringBuilder = builder.append(account).append(cookieid).append(sessionId).append(imei).append(deviceId).append(androidId)
			val str: String = stringBuilder.replaceAllLiterally("null", "")
			StringUtils.isNotBlank(str)
		})

		//加载本地字典
		val areaDic: DataFrame = spark.read.parquet("data_warehouse/data/tmp/area_dic")
		val locMap: Map[String, (String, String, String)] = areaDic.map(row => {
			val LocGeo: String = row.getAs[String]("_1")
			val province: String = row.getAs[String]("_2")
			val city: String = row.getAs[String]("_3")
			val distract: String = row.getAs[String]("_4")
			(LocGeo, (province, city, distract))
		}).collect().toMap

		//加载商圈字典
		val bizDic: DataFrame = spark.read.parquet("data_warehouse/data/tmp/biz_dic")
		val bizMap: Map[String, (String, String, String, String)] = bizDic.map(row => {
			val LocGeo: String = row.getAs[String]("_1")
			val province: String = row.getAs[String]("_2")
			val city: String = row.getAs[String]("_3")
			val distract: String = row.getAs[String]("_4")
			val biz: String = row.getAs[String]("_5")
			(LocGeo, (province, city, distract, biz))
		}).collect().toMap

		//将字典广播
		val locBD: Broadcast[Map[String, (String, String, String)]] = spark.sparkContext.broadcast(locMap)
		val bizBD: Broadcast[Map[String, (String, String, String, String)]] = spark.sparkContext.broadcast(bizMap)

		val res: Dataset[LogBean] = filtered.map(bean => {
			val lng: Double = bean.longtitude
			val lat: Double = bean.latitude
			val geo2Loc = GeoHash.withCharacterPrecision(lat, lng, 5).toBase32
			val geo2Biz = GeoHash.withCharacterPrecision(lat, lng, 6).toBase32

			//获取广播变量
			val locDic: Map[String, (String, String, String)] = locBD.value
			val bizDic: Map[String, (String, String, String, String)] = bizBD.value

			//获取bizDic中的省市县
			val logBiz: (String, String, String, String) = bizDic.getOrElse(geo2Biz, ("", "", "", ""))
			bean.province = logBiz._1
			bean.city = logBiz._2
			bean.district = logBiz._3
			bean.biz = logBiz._4

			//匹配不到biz库匹配locDic
			if (logBiz._1.equals("")) {
				val logLoc: (String, String, String) = locDic.getOrElse(geo2Loc, ("", "", ""))
				bean.province = logLoc._1
				bean.city = logLoc._2
				bean.district = logLoc._3
			}
			bean
		})
		res.filter(bean => bean.province.equals(""))
			.map(bean => (bean.longtitude + "," + bean.latitude))
			.write
			.mode("overwrite")
			.text("data_warehouse/data/tmp/no_match_data")

		//加载停止词词典
		val stWords: DataFrame = spark.read.text("data_warehouse/data/stopwords/stwords.txt")
		val set: Set[Row] = stWords.collect().toSet
		val stWord: Broadcast[Set[Row]] = spark.sparkContext.broadcast(set)
		//标题分词
		res.map(bean => {
			val maybeString: Option[String] = bean.event.get("title")
			if (maybeString != None) {
				val terms: util.List[Term] = HanLP.segment(maybeString.get)
				//过滤掉停止词
				val stopW: Set[Row] = stWord.value
				import scala.collection.JavaConversions._
				val keyword: String = terms.map(term => term.word).filter(word => word.size > 1 && (! stopW.contains(word))).mkString(" ")
				bean.event.+=("keyword" -> keyword)
			}
			bean
		}) .show(1000,false)

		spark.close()
	}
}
