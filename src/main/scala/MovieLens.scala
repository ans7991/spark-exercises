import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object MovieLens {

  case class Movie(movieId: String, title: String, genres: Seq[String])
  case class User(userId: String, name: String)

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("MovieLens with Apache Spark")
    val sc = new SparkContext(conf)

    // USING RDD
    val movies = sc.textFile("src/main/resources/data/movielens/movies.csv").zipWithIndex().filter(_._2 > 0).map(r => r._1)
    val ratings = sc.textFile("src/main/resources/data/movielens/ratings.csv").zipWithIndex().filter(_._2 > 0).map(r => r._1)

    val moviesRatings = ratings.map(r => (r.split(",")(1), r.split(",")(2).toFloat))
                               .groupByKey().map(r => (r._1, r._2.min, r._2.max, r._2.size)).take(10)
    moviesRatings.foreach(println)

    val mostActiveUsers = ratings.map(r => (r.split(",")(0), 1)).reduceByKey(_ + _).sortBy(-_._2).take(10)
    mostActiveUsers.foreach(println)

    val preferredGenresForUser = ratings.filter(r => r.split(",")(0).equals("199") && r.split(",")(2).toFloat >= 4)
                                        .map(r => (r.split(",")(1), r.split(",")(0)))
                                        .join(movies.map(m => (m.split(",")(0), m.split(",")(2))))
    preferredGenresForUser.map(p => p._2).groupByKey().foreach(println)

    // USING DATAFRAMES
    val sqlContext = new SQLContext(sc)
    val moviesDF = sqlContext.read.format("com.databricks.spark.csv")
                                    .option("header", "true")
                                    .option("inferSchema", "true")
                                    .load("src/main/resources/data/movielens/movies.csv")
    val usersDF = sqlContext.read.format("com.databricks.spark.csv")
                                .option("header", "true")
                                .option("inferSchema", "true")
                                .load("src/main/resources/data/movielens/users.csv")
    val ratingsDF = sqlContext.read.format("com.databricks.spark.csv")
                                .option("header", "true")
                                .option("inferSchema", "true")
                                .load("src/main/resources/data/movielens/ratings.csv")

    moviesDF.registerTempTable("movies")
    usersDF.registerTempTable("users")
    ratingsDF.registerTempTable("ratings")

    moviesDF.printSchema()
    usersDF.printSchema()
    ratingsDF.printSchema()

    val topActiveUsers = sqlContext.sql("Select users.name, count(*) as count from users " +
                                        "join ratings on ratings.userId = users.id " +
                                        "group by users.name order by count desc limit 10")
    topActiveUsers.show()

    // Recommendation Engine to recommend movie for a user
    val ratingsRDD = ratings.map(_.split(",")).map(r => Rating(r(0).toInt, r(1).toInt, r(2).toDouble)).cache()
    val splits = ratingsRDD.randomSplit(Array(0.8, 0.2), 0L)
    val trainingRDD = splits(0).cache()
    val testRDD = splits(1).cache()

    val model = new ALS().setRank(20).setIterations(10).run(trainingRDD)
    val testRecomdRDD = testRDD.map({
      case Rating(user, movie, rating) => (user, movie)
    })

    println("Predicted rating of user 594 for movie 490 is " + model.predict(594, 490))
    val predictionRDD = model.predict(testRecomdRDD)

    val testRDDWithUserMovieAsKey = testRDD.map({
      case Rating(user, movie, rating) => ((user, movie), rating)
    })
    val predictionRDDWithUserMovieAsKey = predictionRDD.map({
      case Rating(user, movie, rating) => ((user, movie), rating)
    })

    val testRDDJoinPredictionRdd = testRDDWithUserMovieAsKey.join(predictionRDDWithUserMovieAsKey)
    val falsePositives = testRDDJoinPredictionRdd.filter({
      case ((user, movie), (ratingT, ratingP)) => ratingT <= 1 && ratingP >= 4
    })
    println("No. of False Positives: " + falsePositives.count())
    val meanAbsoluteError = testRDDJoinPredictionRdd.map({
      case ((user, movie), (ratingT, ratingP)) => Math.abs(ratingT - ratingP)
    }).mean()

    println("Mean Absolute error in prediction " + meanAbsoluteError)
  }
}