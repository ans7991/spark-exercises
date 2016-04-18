import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._

object Olympics {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Olympics with Apache Spark")
    val sc = new SparkContext(conf)

    // USING RDD
    val csvFile = sc.textFile("src/main/resources/data/olympics.csv")
    val olympics = csvFile.zipWithIndex().filter(_._2 > 0).map(r => r._1)

    val countryWithMostMedals = olympics.map(r => (r.split(",")(1), r.split(",")(7).toInt))
                                        .reduceByKey(_ + _).sortBy(-_._2)
                                        .take(1)
    countryWithMostMedals.iterator.foreach(println)

    val maxMedalYearForCountry = olympics.filter(r => r.split(",")(1).equals("China"))
                                          .map(r => (r.split(",")(2), r.split(",")(7).toInt))
                                          .reduceByKey(_ + _).sortBy(-_._2).take(1)
    maxMedalYearForCountry.iterator.foreach(println)

    val playerWithMostMedals = olympics.map(r => (r.split(",")(0), r.split(",")(7).toInt))
                                        .reduceByKey(_ + _).sortBy(-_._2)
                                        .take(1)
    playerWithMostMedals.iterator.foreach(println)

    // USING DATAFRAMES
    val sqlContext = new SQLContext(sc)
    val olympicsDF = sqlContext.read.format("com.databricks.spark.csv")
                                    .option("header", "true")
                                    .option("inferSchema", "true")
                                    .load("src/main/resources/data/olympics.csv")

    olympicsDF.groupBy("Country").agg(sum("Total").alias("Medals")).orderBy(desc("Medals")).show(10)
    olympicsDF.filter(olympicsDF("Country") === "China").groupBy("Year").agg(sum("Total").alias("Medals")).orderBy(desc("Medals")).show(5)
    olympicsDF.groupBy("Athlete").agg(sum("Total").alias("Medals")).orderBy(desc("Medals")).show(5)

    /*
    (United States,1312)
    (2008,184)
    (Michael Phelps,22)


    +-------------+------+
    |      Country|Medals|
    +-------------+------+
    |United States|  1312|
    |       Russia|   768|
    |      Germany|   629|
    |    Australia|   609|
    |        China|   530|
    |       Canada|   370|
    |        Italy|   331|
    |Great Britain|   322|
    |  Netherlands|   318|
    |       France|   318|
    +-------------+------+
    only showing top 10 rows

    +----+------+
    |Year|Medals|
    +----+------+
    |2008|   184|
    |2012|   125|
    |2004|    94|
    |2000|    79|
    |2010|    19|
    +----+------+
    only showing top 5 rows

    +--------------------+------+
    |             Athlete|Medals|
    +--------------------+------+
    |      Michael Phelps|    22|
    |    Natalie Coughlin|    12|
    |         Ryan Lochte|    11|
    |        Leisel Jones|     9|
    |Ole Einar Bjørndalen|     9|
    +--------------------+------+
    only showing top 5 rows
    */
  }
}
