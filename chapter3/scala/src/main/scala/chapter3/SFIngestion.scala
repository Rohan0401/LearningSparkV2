package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.)

object SFIngestion {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
                .builder
                .appName("SFIngestion")
                .getOrCreate()
    val sampleDF = spark
      .read
      .option("samplingRatio", 0.001)
      .option("header", true)
      .csv("""/databricks-datasets/learning-spark-v2/
             |sf-fire/sf-fire-calls.csv""")
    print(sampleDF.show(5))
  }

}