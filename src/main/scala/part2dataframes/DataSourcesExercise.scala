package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSourcesExercise extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("DataSources Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //tab-separated
  moviesDF.write
    .option("header", "true")
    .option("sep", "\t")
    .csv("src/main/resources/data/movies_tab_separated.csv")

  //parquet
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/movies.parquet")

  //postgres
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()

}
