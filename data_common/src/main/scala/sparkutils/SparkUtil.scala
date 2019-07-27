package sparkutils

import org.apache.spark.sql.SparkSession

object SparkUtil {
	def getSparkSession(appName:String,master:String="local[*]"): SparkSession ={
		SparkSession.builder().appName(appName).master(master).getOrCreate()
	}
}
