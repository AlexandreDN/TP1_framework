import org.apache.spark.{SparkConf, SparkContext}

object Main {


  def main(args: Array[String]): Unit = {

    //Allouer de la mémoire
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark Pi").set("spark.driver.host", "localhost")
    conf.set("spark.testing.memory", "2147480000")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    //****************************************************



  }
}