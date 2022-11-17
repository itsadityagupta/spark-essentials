package part2dataframes

import org.apache.spark.sql.SparkSession

object DataFramesBasicsExercise extends App {

  /**
    * Exercise-1
    */

  val spark: SparkSession = SparkSession.builder()
    .appName("DataFramesBasicsExercise")
    .config("spark.master", "local")
    .getOrCreate()

  val smartphones = Seq(
    ("make1", "type1", "model1", "16MP"),
    ("make2", "type2", "model2", "12MP"),
    ("make3", "type3", "model3", "56MP"),
    ("make4", "type4", "model4", "12MP"),
  )

  val smartphonesDF = spark.createDataFrame(smartphones)
  smartphonesDF.printSchema()

  //using implicits
  import spark.implicits._
  val smartphonesDFImplicit = smartphones.toDF("Make", "Type", "Model", "Camera")

  smartphonesDFImplicit.printSchema()

  /**
    * Exercise-2
    */

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()

  println(s"Total movie counts: ${moviesDF.count()}")
}
