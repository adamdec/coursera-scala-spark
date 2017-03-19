import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldTest {

  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Wiki app")
  val sc: SparkContext = new SparkContext(conf)

  val l = (
    for (l <- ('A' to 'z')
    ) yield l.toString()).toList

  val myRdd2: RDD[String] = sc.parallelize(l, 2)

  def foldTest(rdd: RDD[String]): String = rdd.fold("")(_ + _)

  def main(args: Array[String]) {
    println("Orig rdd: " + myRdd2.collect().toList)
    println("Fold : " + foldTest(myRdd2))
  }
}