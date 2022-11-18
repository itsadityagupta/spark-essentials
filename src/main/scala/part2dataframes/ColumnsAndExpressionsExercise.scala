package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object ColumnsAndExpressionsExercise extends App {

  val spark = SparkSession.builder()
    .appName("ColumnsAndExpressionsExercise")
    .config("spark.master", "local")
    .getOrCreate()

  //inferSchema is true by default
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  moviesDF.printSchema()

  // 1
  import spark.implicits._
  val moviesColumnsDF1 = moviesDF.select('Director, col("Major_Genre"))
  moviesColumnsDF1.printSchema()

  //2
  val moviesProfitDF = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )
  moviesProfitDF.take(10).foreach(println)

  //3

  moviesDF.select("Major_Genre").distinct().show()

  val comedyMovies = moviesDF
    .filter("Major_Genre = 'Comedy' or Major_Genre = 'Black Comedy' or Major_Genre = 'Romantic Comedy'")
    .filter("IMDB_Rating > 6")

  print(s"Movies Found: ${comedyMovies.count()}")

  comedyMovies.show()

}
