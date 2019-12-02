package utils

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkUtils {

  lazy val spark = SparkSession.builder().appName("Iconic App").master("local[*]").getOrCreate()

  def getDataFrame(path: String, schema: StructType) : DataFrame = {
    this.spark.read
      .option("delimiter", "\t")
      .schema(schema)
      .csv(path)
  }
}
