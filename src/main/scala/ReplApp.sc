import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
  .appName("Iconic App")
  .master("local[*]")
  .getOrCreate()

print(spark.getClass)