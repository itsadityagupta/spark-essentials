package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinsExercise extends App {

  val spark = SparkSession.builder()
    .appName("JoinsExercise")
    .config("spark.master", "local")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  val salariesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.salaries")
    .load()

  val deptManagerDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.dept_manager")
    .load()

  val titles = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.titles")
    .load()

//  println(s"Employee count: ${employeesDF.count()}")
//  println(s"Manager count: ${deptManagerDF.count()}")

//  employeesDF.printSchema()
//  employeesDF.show()
//
//  salariesDF.printSchema()
//  salariesDF.show()
//
//  deptManagerDF.printSchema()
//  deptManagerDF.show()

  // 1

  /**
    * My solution:
  val windowFunction = Window.partitionBy("emp_no").orderBy(desc("salary"))
  val rankedSalariesDF = salariesDF.withColumn("row_num", row_number().over(windowFunction))
  val maxSalariesDF = rankedSalariesDF
    .filter(col("row_num") === 1) // max salaries will have rank 1
    .drop("row_num")
    .withColumnRenamed("salary", "max_salary")

  val joinCondition = employeesDF.col("emp_no") === maxSalariesDF.col("emp_no")

  val employeesMaxSalariesDF = employeesDF
    .join(maxSalariesDF, joinCondition, "left_outer")
    .drop(maxSalariesDF.col("emp_no"))

  employeesMaxSalariesDF.show()

    */
  // given solution:
  val maxSalariesDF = salariesDF.groupBy("emp_no").agg(max("salary").as("max_salary"))
  val employeesMaxSalariesDF = employeesDF
    .join(maxSalariesDF, "emp_no")

  employeesMaxSalariesDF.show()


  // 2

  val joinCondition2 = employeesDF.col("emp_no") === deptManagerDF.col("emp_no")
  val employeesNeverManagerDF = employeesDF.join(deptManagerDF, joinCondition2, "left_anti")

  employeesNeverManagerDF.show()

  println(s"Never Manager count: ${employeesNeverManagerDF.count()}")

  // 3

  /**
    * My solution

  val bestPaidEmployees = employeesMaxSalariesDF
    .orderBy(col("max_salary").desc)
    .limit(10)

  val windowFunctionTitles = Window.partitionBy("emp_no").orderBy(col("from_date").desc)
  val recentTitlesRanked = titles.withColumn("row_num", row_number().over(windowFunctionTitles))
  val recentTitles = recentTitlesRanked
    .filter(col("row_num") === 1)
    .drop(col("row_num"))

  val joinCondition3 = bestPaidEmployees.col("emp_no") === recentTitles.col("emp_no")
  val titlesBestPaid = bestPaidEmployees
    .join(recentTitles, joinCondition3)
    .drop(titles.col("emp_no"))
//    .select(bestPaidEmployees.col("emp_no"), titles.col("title"))

  titlesBestPaid.show()
  */

  //given solution

  val recentTitles = titles
    .groupBy("emp_no")
    .agg(max("to_date").as("to_date"))
    .select("emp_no", "title")

  recentTitles.show()

  val bestPaidEmployees = employeesMaxSalariesDF
    .orderBy(col("max_salary").desc)
    .limit(10)

  val bestPaidJobs = bestPaidEmployees.join(recentTitles, "emp_no")
  bestPaidJobs.show()

  // my solution is correct. the given solution is wrong as it gives the best paid employees for all the emp-job title pairs.

}
