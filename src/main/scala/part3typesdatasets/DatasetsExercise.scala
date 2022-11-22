package part3typesdatasets

import org.apache.spark.sql.SparkSession

object DatasetsExercise extends App {

  val spark = SparkSession.builder()
    .appName("DatasetsExercise")
    .config("spark.master", "local")
    .getOrCreate()

  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  val carsDF = spark.read.option("inferSchema", "true").json(s"src/main/resources/data/cars.json")
  import spark.implicits._
  val carsDS = carsDF.as[Car]

  val count = carsDS.count()
  println(s"Count: $count")

  val powerfulCarsCount = carsDS.filter(car => car.Horsepower.getOrElse(0L) > 140).count()
  println(s"Powerful Cars Count: $powerfulCarsCount")

  val avgHorsePowerCount = carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / count
  println(s"Avg horse power: $avgHorsePowerCount")

}
