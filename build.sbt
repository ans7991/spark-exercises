name := "spark_exercises"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.1"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"