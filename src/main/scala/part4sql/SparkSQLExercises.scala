package part4sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSQLExercises extends App {

  val spark = SparkSession.builder()
    .appName("SparkSQLExercises")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")

  def readTable(tableName: String): DataFrame =
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"public.$tableName")
      .load()

  val moviesDF = readTable("movies")
  val employeesDF = readTable("employees")
  val deptDF = readTable("dept_emp")
  val salariesDF = readTable("salaries")
  val deptNameDF = readTable("departments")

  moviesDF.createOrReplaceTempView("movies")
  employeesDF.createOrReplaceTempView("employees")
  deptDF.createOrReplaceTempView("dept_emp")
  salariesDF.createOrReplaceTempView("salaries")
  deptNameDF.createOrReplaceTempView("departments")


  moviesDF.write.mode(SaveMode.Overwrite).saveAsTable("movies")


  spark.sql(
    """
      |select count(*) as Ans
      |from employees e
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |""".stripMargin)
    .show()

  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, dept_emp de, salaries s
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and e.emp_no = s.emp_no
      |group by de.dept_no
      |""".stripMargin)
    .show()

  spark.sql(
    """
      |select d.dept_name, avg(s.salary) avg_salary
      |from employees e, dept_emp de, salaries s, departments d
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and e.emp_no = s.emp_no
      |and de.dept_no = d.dept_no
      |group by d.dept_name
      |order by avg_salary desc
      |limit 1
      |""".stripMargin)
    .show()


}
