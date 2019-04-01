import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val logFile = "/home/bob/gtd/Iconic/test.txt" // Should be some file on your system
    val spark = SparkSession.builder
      .appName("Iconic App")
      .master("local[*]")
      .config("log4j.rootCategory","WARN, console")
      .getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}