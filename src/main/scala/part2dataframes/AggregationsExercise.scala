package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationsExercise extends App {

  val spark = SparkSession.builder()
    .appName("AggregationsExercise")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  moviesDF.printSchema()

  // 1
  val movieProfitsDF = moviesDF
    .select((col("US_gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Profit"))
    .agg(sum("Total_Profit"))

  movieProfitsDF.show()

  // 2
  val directorsCount = moviesDF.select(countDistinct("Director").as("nDirectors"))
  directorsCount.show()

  // 3

  val statsMoviesDF = moviesDF.agg(
    mean("US_gross").as("Avg US revenue"),
    stddev("US_gross").as("STD US revenue")
  )
  statsMoviesDF.show()

  // 4
  val perDirectorStatsDF = moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg IMDB Rating"),
      avg("US_gross").as("Avg US Revenue")
    )
    .orderBy(desc("Avg IMDB Rating"))
//    .orderBy(col("Avg IMDB Rating").desc_nulls_last)
  perDirectorStatsDF.show()


}
