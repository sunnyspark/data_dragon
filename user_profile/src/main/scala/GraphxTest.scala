import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkutils.SparkUtil

import scala.collection.immutable.StringOps

object GraphxTest {
	Logger.getLogger("org").setLevel(Level.WARN)
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkUtil.getSparkSession("GraphX")
		val dataFrame: DataFrame = spark.read.option("header","true").csv("user_profile/data/graphx/graphx.txt")
		//构成点集合
		val pointRDD: RDD[(Long, String)] = dataFrame.rdd.flatMap(row => {
			val phone: String = row.getAs[String](0)
			val name: String = row.getAs[String](1)
			val wx: String = row.getAs[String](2)

			//凭借成一个集合
			val lst: List[String] = phone :: name :: wx :: Nil
			//过滤是否为空,并构成点集合
			for (elem <- lst if (StringUtils.isNotBlank(elem))) yield (elem.hashCode.toLong, elem)
		})

		//构成边集合
		val borderRDD: RDD[Edge[String]] = dataFrame.rdd.flatMap(row => {
			val phone: String = row.getAs[String](0)
			val name: String = row.getAs[String](1)
			val wx: String = row.getAs[String](2)

			val lst: List[String] = phone :: name :: wx :: Nil
			val filtered: List[String] = lst.filter(StringUtils.isNotBlank(_))

			//取出集合中的头一次与为组合,edge构成边集合
			val head: StringOps = filtered.head
			val tail: List[String] = filtered.tail
			for (elem <- tail) yield Edge[String](head.hashCode().toLong, elem.hashCode.toLong, "")
		})

		//构造图
		val graph: Graph[String, String] = Graph(pointRDD,borderRDD)

		//调用api得到各组链接通子图
		val vertexRDD: VertexRDD[VertexId] = graph.connectedComponents().vertices

		val result = vertexRDD.join(pointRDD.distinct()).map(_._2).groupByKey().mapValues(_.toList)
		result.foreach(println)
		spark.close()
	}
}
