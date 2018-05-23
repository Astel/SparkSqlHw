package com.epam.hubd.scala.spark.sql.operation

import com.epam.hubd.scala.spark.sql.domain.{EsotericPerson, Person, Zodiac}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Application {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .enableHiveSupport
      .master("local[4]")
      .appName("spark-sql-operations-scala")
      .getOrCreate

    val sc = spark.sparkContext

    val sqlContext = spark.sqlContext

    import sqlContext.implicits._

    // Dependencies
    // com.databricks:spark-csv_2.10:1.4.0

    // DataSource API
    // Load
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/home/ferenc_kis/Projects/datasets/people")

    df.show
    df.printSchema

    // Save
    df.write
      .mode(SaveMode.Overwrite)
      .json("/home/ferenc_kis/Projects/datasets/people.json")

    // automatic schema inferenc with Datasource API
    val jsonDF = sqlContext.read
      .json("/home/ferenc_kis/Projects/datasets/people.json")

    jsonDF.show
    jsonDF.printSchema

    // source RDD to demnostrate DF creation
    val rdd = sc.textFile("/home/ferenc_kis/Projects/datasets/people")
      .filter(!_.equalsIgnoreCase("id,name,age"))
      .map(_.split(","))

    // DF creation through reflection
    val reflectionDF = rdd.map(p => Person(p(0).toInt, p(1), p(2).toInt))
      .toDF

    reflectionDF.show
    reflectionDF.printSchema

    // DF creation through programmatic schema infer
    val schema = StructType(Array(StructField("name", IntegerType), StructField("name", StringType), StructField("age", IntegerType)))
    val programmaticDF = sqlContext.createDataFrame(rdd.map(p => Row(p(0).toInt, p(1), p(2).toInt)), schema)

    programmaticDF.show
    programmaticDF.printSchema

    // DataFrame DSL functions
    df.select("name")
      .show

    df.select(df.col("age"), $"name", df("id"))
      .show

    // functions
    // some function accepts string as parameter
    df.select(lpad($"id", 8, "0").as("paddedId"), upper($"name").as("upperName"), abs($"age" - 2016).as("birthYear"))
      .show

    // when you only want to add a column
    df.withColumn("firstName", split($"name", "\\s")(0))
      .show

    // when you only want to remove a column
    df.drop($"id")
      .show

    // filtering
    df.filter($"age".between(14, 18))
      .show

    df.where("name like '%Swanson'")
      .show

    // groupby and col functions
    df.withColumn("firstName", split($"name", "\\s")(0))
      .groupBy($"firstName")
      .agg(max($"age"), min($"age"), count($"age"))
      .show

    // predefined aggregation
    df.groupBy($"name")
      .max("age")
      .limit(5)
      .show

    // window function
    df.withColumn("AgeGroup",
      when($"age" < 16, "Children")
        .when($"age" < 25, "Youth")
        .when($"age" < 65, "Adult")
        .otherwise("Seniors"))
      .where($"name" like "Kate %")
      .select($"name", rank.over(Window.partitionBy($"AgeGroup").orderBy($"age")).as("rank"))
      .where($"rank" < 4)
      .show


    // explode
    df.withColumn("ageInDays", $"age" * 365)
      .explode($"age", $"ageInDays")(row => Array(("ageInYears", row.getInt(0)), ("ageInDays", row.getInt(1))))
      .show

    // distinct and orderby (sort)
    df.select($"age")
      .distinct
      .orderBy($"age")
      .show

    // join - Chinese horoscope
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/home/ferenc_kis/Projects/datasets/zodiac")
      .join(df.withColumn("yearOfBirth", abs($"age" - 2016)), $"yearOfBirth" === $"year")
      .select($"name", $"yearOfBirth", $"zodiac")
      .show

    // back to RDD
    df.rdd.map(_.getString(1)).take(10)
      .foreach(println)

    // demonstrate SQL
    df.createOrReplaceTempView("persons")
    sqlContext.sql("SELECT split(name,'\\s')[0] as firstname, min(age), max(age)" +
      " FROM persons" +
      " WHERE name LIKE 'Lu%'" +
      " GROUP BY split(name,'\\s')[0]" +
      " ORDER BY min(age) DESC")
      .show
    sqlContext.dropTempTable("persons")

    // SQL from files
    sqlContext.sql("SELECT * FROM text.`/home/ferenc_kis/Projects/datasets/zodiac`")
      .show

    // udfs
    def getAgeGroup = (age: Int) => age match {
      case `age` if age < 16 => "Children"
      case `age` if age < 25 => "Youth"
      case `age` if age < 65 => "Adult"
      case _ => "Seniors"
    }

    // dataframe dsl
    def getAgeGroupUDF = udf(getAgeGroup)
    df.groupBy(getAgeGroupUDF($"age").as("ageGroup"))
      .count
      .show

    // sql
    sqlContext.udf.register("getAgeGroup", getAgeGroup)
    df.registerTempTable("persons")
    sqlContext.sql("SELECT getAgeGroup(age) as ageGroup, count(*) as count" +
      " FROM persons" +
      " GROUP BY getAgeGroup(age)")
      .show
    sqlContext.dropTempTable("persons")


    // HIVE interoperability
    df.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("people")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS seniors (id INT, name STRING, age INT)")
    sqlContext.sql("SHOW tables")
      .show
    sqlContext.sql("INSERT OVERWRITE TABLE seniors" +
      " SELECT * FROM people" +
      " WHERE age > 65")
    sqlContext.sql("SELECT * FROM seniors")
      .show
    sqlContext.sql("DROP TABLE people")
    sqlContext.sql("DROP TABLE seniors")
    sqlContext.sql("SHOW tables")
      .show

    // Parquet
    df.withColumn("yearOfBirth", abs($"age" - 2016))
      .write
      .partitionBy("yearOfBirth")
      .mode(SaveMode.Overwrite)
      .parquet("/home/ferenc_kis/Projects/datasets/people.parquet")

    sqlContext.read
      .parquet("/home/ferenc_kis/Projects/datasets/people.parquet")
      .where($"yearOfBirth" === 1965)
      .select($"name")
      .show


    // Catalyst query optimization
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/home/ferenc_kis/Projects/datasets/zodiac")
      .join(df.withColumn("yearOfBirth", abs($"age" - 2016)), $"yearOfBirth" === $"year")
      .select($"name", $"yearOfBirth", $"zodiac")
      .filter($"name" like "Sam%")
      .explain(true)

    // To DataSet
    val shortestNamesByYearOfBirth = df.as[Person]
      .filter(_.name startsWith "Sam")
      .map(p => Person(p.id, p.name, 2016 - p.age))

    val zodiac = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/home/ferenc_kis/Projects/datasets/zodiac")
      .as[Zodiac]

    val ds = shortestNamesByYearOfBirth.joinWith(zodiac, $"age" === $"year")
      .map { case (l, r) => EsotericPerson(l.name, l.age, r.zodiac) }

    ds.printSchema
    ds.show(10)
  }
}
