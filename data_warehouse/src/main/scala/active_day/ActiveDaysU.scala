package active_day

import org.apache.spark.sql.SparkSession
import sparkutils.SparkUtil

object ActiveDaysU {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
		spark.read.
	}
}
