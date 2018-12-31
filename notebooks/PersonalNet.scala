// Databricks notebook source
dbutils.widgets.dropdown("debug", "true", Seq("true", "false"))

// COMMAND ----------

val debug = dbutils.widgets.get("debug").toBoolean

// COMMAND ----------

// MAGIC %md
// MAGIC # Create the personal net

// COMMAND ----------

// Credentials

val client_id = sys.env("AZURE_CLIENT_KEY")
val client_secret = sys.env("AZURE_SECRET_KEY")
val tenant_id = sys.env("AZURE_TENANT_ID")

// Spark-to-ADLS configurations for Dataframes and Datasets
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", client_id)
spark.conf.set("dfs.adls.oauth2.credential", client_secret)
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/" + tenant_id + "/oauth2/token")

// Spark-to-ADLS configurations for RDD
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.client.id", client_id)
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.credential", client_secret)
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/" + tenant_id + "/oauth2/token")

// path variables
val adls = "magdls"     // adls name
val coauthors_dir = "graph/result/CoautoriNeagregat_Clean"  // directory with the mag data
val write_dir = "graph/result"
val coauthDB = "adl://" + adls + ".azuredatalakestore.net/" + coauthors_dir // coauthors database
val wmag = "adl://" + adls + ".azuredatalakestore.net/" + write_dir

// COMMAND ----------

// reading coauthor data 
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection

def getDataFrame(path: String, schema: StructType) : DataFrame = {
  spark.read
    .option("delimiter", ",")
    .schema(schema)
    .csv(path)
}
//define a case class to store information about each row of the csv file
/// Coauthor 
case class Coauthor(ego: Long, year: Int, alter: Long)
val coauthorSchema = ScalaReflection.schemaFor[Coauthor].dataType.asInstanceOf[StructType]
val CoauthorsDB = getDataFrame(coauthDB, coauthorSchema)



// COMMAND ----------

// MAGIC %md
// MAGIC # Adding test data

// COMMAND ----------

val timeout = 60
if(dbutils.notebook.run("AddTestData", timeout) != "OK" ) {
    dbutils.notebook.exit("Error")
}

// COMMAND ----------

// datasets
val globalTempDb = spark.conf.get("spark.sql.globalTempDatabase")
val dfPapers = table(globalTempDb + ".papers")
val dfAuthors = table(globalTempDb + ".authors")
val dfPaperAuthorAff = table(globalTempDb + ".paa")
val dfPaperReferences = table(globalTempDb + ".pr")
// some of the paper entries have a null year
// we will replace them with a big value so they won't bother us when sorting ascendingly by year
import org.apache.spark.sql.DataFrameNaFunctions
val dfsPapers = dfPapers.na.fill(3000,Seq("year"))

// COMMAND ----------

import org.apache.spark.sql.functions.{collect_list, collect_set, lit, explode, concat_ws}

val tfPaperAuthors = dfPaperAuthorAff.groupBy("paper").agg(collect_list($"author").as("authors"))
val papersAuthorsWithYears = tfPaperAuthors.join(dfsPapers, $"id" === $"paper")//.select("id", "year", "authors")//.withColumn("author",lit(0L))

val coauthorsByYear = papersAuthorsWithYears.select(explode($"authors").as("author"), $"year", $"authors")
val flatten = udf((xs: Seq[Seq[Long]]) => xs.flatten.distinct)
val tfCoauthors = coauthorsByYear.groupBy("author", "year")
  .agg(flatten(collect_list("authors")).as("authors"))
  .select($"author".as("ego"), $"year", explode($"authors").as("alter"))
  .filter($"ego" =!= $"alter") // drop duplicates
  

// COMMAND ----------

// MAGIC %md
// MAGIC #Building the personal network

// COMMAND ----------

var dfCoauthors = spark.emptyDataFrame
if(debug){
  dfCoauthors = tfCoauthors
}
else{
  dfCoauthors = CoauthorsDB
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC a personal net is defined by the ego and pairs of its alters who worked together
// MAGIC the year of collaboration between alters in the personal net will be the maximum of three :
// MAGIC     - first year of collab. between Ego and Alter1
// MAGIC     - first year of collab. between Ego and Alter2
// MAGIC     - first year of collab. between Alter1 and Alter2

// COMMAND ----------

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{greatest, min, collect_list, udf, explode, arrays_zip}
// keep only the first year two authors worked together
val coauthorsMinYear = dfCoauthors.groupBy("ego", "alter").agg(min($"year").as("year"))
// create all pairs of alters and keep the maximum of their collaboration year with the ego

// cross product scala
implicit class Crossable[X](xs: Traversable[X]) {
  def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
}
// val zip = udf((xs: Seq[Long], ys: Seq[Int]) => xs.zip(ys))
// case class AuthorYear(_1: Long, _2: Int)
case class AlterPair(ego: Long, a1: Long, a2: Long, year: Int)

val coauthorsMinYearList = coauthorsMinYear
  .groupBy("ego")
  .agg(collect_list($"alter").as("alters"), collect_list($"year").as("years"))

// val alter_pairs = udf(
//   (ego: Long, alters: Seq[Long], years: Seq[Int]) => {
//     val alterYears = alters.zip(years)
//     val alters_cross = alterYears cross alterYears
//     val filtered_alter_cross = alters_cross.filter( pair => pair._1._1 < pair._2._1)

//     val alter_pairs = filtered_alter_cross.map( pair => {
//       if(pair._1._2 > pair._2._2){
//         (pair._1._1, pair._2._1, pair._1._2)
//       }
//       else{
//         (pair._1._1, pair._2._1, pair._2._2)
//       }
//     })
//     alter_pairs.toSeq
//     2
//   }
// )

import org.apache.spark.sql.catalyst.ScalaReflection
val schema = ScalaReflection.schemaFor[AlterPair].dataType.asInstanceOf[StructType]
val rowEncoder = RowEncoder(schema)
val alterPairs = coauthorsMinYearList.flatMap {
  (row : Row ) => {
    val ego = row.getLong(0)
    val alters : Seq[Long] = row.getSeq(1)
    val years : Seq[Int] = row.getSeq(2)
    val alterYears = alters zip years
    val altersCross = alterYears cross alterYears
    val altersCrossFiltered = altersCross.filter( pair =>  pair._1._1 < pair._2._1)
    var year : Int = 0
    val alterPairs = altersCrossFiltered.map(pair => {
      val a1 = pair._1._1
      val a2 = pair._2._1
      if(pair._1._2 > pair._2._2){
        year = pair._1._2
      }
      else{
        year = pair._2._2
      }
      Row(ego, a1, a2, year)
    })
    alterPairs
  }} (rowEncoder)

// coauthorsMinYearList
//   .select(alter_pairs($"ego", $"alters", $"years").as("alter_pairs"))
//   .select($"ego",$"alter_pairs._1".as("a1"), $"alter_pairs._2".as("a2"), $"alter_pairs._3".as("year"))

// COMMAND ----------

val dfPersonalNet = coauthorsMinYear.as("C")
  .join(alterPairs.as("AP"), $"AP.a1" === $"C.ego" && $"AP.a2" === $"C.alter")
  .withColumn("maxYear", greatest($"AP.year", $"C.year"))
  .select($"AP.ego".as("ego"), $"AP.a1".as("alter1"), $"AP.a2".as("alter2"), $"maxYear".as("year"))

// COMMAND ----------

// dfPersonalNet.collect

// COMMAND ----------

// path variables
val adls = "magdls"     // adls name
val write_dir = "graph/result" //directory to which we write files
val wmag = "adl://" + adls + ".azuredatalakestore.net/" + write_dir

// COMMAND ----------

alterPairs.write.format("com.databricks.spark.csv").save(wmag + "/AlterPairs")

// COMMAND ----------

dfPersonalNet

// COMMAND ----------


// fileCoauthors.write.partitionBy("author").format("com.databricks.spark.csv").save(wmag + "/output_1")
//fileCoauthors.coalesce(1).write.partitionBy("author").format("com.databricks.spark.csv").save(wmag + "/output1")
dfPersonalNet.write.format("com.databricks.spark.csv").save(wmag + "/PersonalNet_2")