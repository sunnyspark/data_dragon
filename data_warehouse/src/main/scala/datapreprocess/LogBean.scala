package datapreprocess

case class LogBean(
	                  cookieid: String,
	                  account: String,
	                  imei: String,
	                  osName: String,
	                  osVer: String,
	                  resolution: String,
	                  androidId: String,
	                  manufacture: String,
	                  deviceId: String,
	                  appid: String,
	                  appVer: String,
	                  release_ch: String,
	                  promotion_ch: String,
	                  areacode: String,
	                  longtitude: Double,
	                  latitude: Double,
	                  carrier: String,
	                  netType: String,
	                  sessionId: String,
	                  logType: String,
	                  commit_time: Long,
	                  var event:Map[String, String],
	                  var province:String = "",
	                  var city:String = "",
	                  var district:String = "",
	                  var biz:String = ""

                  )
