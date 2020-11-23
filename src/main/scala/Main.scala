import org.apache.commons.math3.stat.descriptive.moment.Mean
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Main {


  def main(args: Array[String]): Unit = {

    //Allouer de la mémoire
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark Pi").set("spark.driver.host", "localhost")
    conf.set("spark.testing.memory", "2147480000")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    //****************************************************

    ///Exercice 1
    //question 1
    Logger.getLogger("org") setLevel (Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    val rdd = sparkSession.sparkContext.textFile("donnees.csv")
    print("\nQuestion 1\n")
    rdd.foreach(println)

    //question 2
    print("\nQuestion 2\n")
    val DiCaprio_rdd=rdd.filter(item => (item.split(";")(3).contains("Di Caprio")))
    print(DiCaprio_rdd.count())

    //question 3
    print("\nQuestion 3\n")
    val DiCaprio_Note = DiCaprio_rdd.map(item =>(item.split(";")(2).toDouble))
    val mean= DiCaprio_Note.sum() / DiCaprio_Note.count()
    print(mean)

    //question 4
    print("\nQuestion 4\n")
    val DiCaprio_views=DiCaprio_rdd.map(item => (item.split(";")(1).toDouble))
    val allViews = rdd.map(item => (item.split(";")(1).toDouble))
    val rateView= DiCaprio_views.sum() / allViews.sum() * 100
    print(rateView)

    //question 5
    print("\n Question 5\n")
    val counts = rdd.map(item => (item.split(";")(3).toString, (1.0, item.split(";")(2).toDouble)) )
    val countSums = counts.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    val keyMeans = countSums.mapValues(avgCount => avgCount._2 / avgCount._1)
    keyMeans.foreach(println)





    





    ///Exercice 2
    ///question 1
    val df: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("donnees.csv")

    //question 2
    val df_colums_renamed: DataFrame = df.withColumnRenamed("_c0", "nom_film")
      .withColumnRenamed("_c1", "nombre_vues")
      .withColumnRenamed("_c2", "note_film")
      .withColumnRenamed("_c3", "acteur_principal")

    //question 3
    val DiCaprio_movies: DataFrame = df_colums_renamed.filter(col("acteur_principal").contains("Di Caprio"))
    DiCaprio_movies.show

    val DiCaprio_Means_Notes: DataFrame = DiCaprio_movies.groupBy( col1 = "acteur_principal").mean( colNames = "note_film")
    DiCaprio_Means_Notes.show

    val Movies_Allviews  = df_colums_renamed.groupBy().sum("nombre_vues").first.get(0).toString.toDouble
    val DiCaprio_Movies_Allviews = DiCaprio_movies.groupBy().sum("nombre_vues").first.get(0).toString.toDouble
    val pourcentage_vues_ldc: Double = DiCaprio_Movies_Allviews / Movies_Allviews * 100
    print(pourcentage_vues_ldc)

    val Actors_Means_Notes = df_colums_renamed.groupBy( col1 = "acteur_principal").mean( colNames = "nombre_vues")
    Actors_Means_Notes.show

    val Actors_Means_Views = df_colums_renamed.groupBy( col1 = "acteur_principal").mean( colNames = "note_film")
    Actors_Means_Views.show

    //question 4
    val pourcentage_vues = df_colums_renamed.withColumn(colName = "pourcentage_de_vues", col(colName = "nombre_vues") / Movies_Allviews * 100)
    pourcentage_vues.show




  }
}