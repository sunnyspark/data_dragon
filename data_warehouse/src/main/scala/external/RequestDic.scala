package external

import java.io.{BufferedReader, BufferedWriter, File, FileWriter, InputStream, InputStreamReader, OutputStreamWriter}

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpUriRequest}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}

object RequestDic {

	def main(args: Array[String]): Unit = {
		val url = "https://restapi.amap.com/v3/geocode/regeo?"
		val key = "a21304e65185618370cbc2ae9be3a03d"
		val lng_lat = "116.481488,39.990464"
		val parametersMap = Map[String, String](("key", key), ("location", lng_lat), ("batch", "false"))
		val parameters: String = parametersMap.map(tp => (tp._1 + "=" + tp._2)).mkString("&")

		//请求
		val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
		val httpGet: HttpGet = new HttpGet(url + parameters)
		// println(httpGet)
		val response: CloseableHttpResponse = httpClient.execute(httpGet)

		//获取请求中的数据
		val content: InputStream = response.getEntity.getContent
		val reader: BufferedReader = new BufferedReader(new InputStreamReader(content))
		val writer: BufferedWriter = new BufferedWriter(new FileWriter(new File("data_warehouse/data/gd_dic")))
		var line = ""
		val buffer: StringBuffer = new StringBuffer()
		while (line != null) {
			line = reader.readLine()
			if (line != null) {
				buffer.append(line)
			}
		}

		reader.close()
	}
}
