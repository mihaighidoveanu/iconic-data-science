// Databricks notebook source
// MAGIC %md
// MAGIC # Reducing the Microsoft Academic Graph to a compact version
// MAGIC 
// MAGIC The aim of this notebook is to create for each researcher in the MAG a table whose rows are indexed by the researcher's coauthors, and columns (`10`) indicate the number of articles coauthored by the two for each of the years considered. 
// MAGIC 
// MAGIC In addition to that, to keep track of citation counts, the table will contain an extra row (indexed by, say `0`) containing for each of the year the total citation count for the author in that year.

// COMMAND ----------

val timeout = 60
val debug = false
if(debug == true){
  if(dbutils.notebook.run("AddTestData", timeout) != "OK" ) {
    dbutils.notebook.exit("Error")
  }
}
else {
   if(dbutils.notebook.run("SetUpEnv", timeout, Map("debug" -> debug.toString)) != "OK" ) {
    dbutils.notebook.exit("Error")
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Initial Setup
// MAGIC After the setup we can access the following dataframes, with the data schemes [here](https://microsoftdocs.github.io/MAG/Mag-ADLS-Schema):
// MAGIC + dfPapers, dfAuthors, dfPaperAuthorAff 
// MAGIC + dfFos, dfPaperFos, dfFosChildren
// MAGIC + dfPaperReferences

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

//case class PaperAuthor(paper : Long, author: Long, authors : Array[Long])
//dfPaperAuthorAff.groupByKey(_.paper)
//val dfPaperAuthor = dfPaperAuthorAff.select("paper", "author").withColumn("authors",lit(Array[Long]())).as[PaperAuthor]
//dfPaperAuthor.groupByKey(_.paper).reduceGroups( (accumulated, current) => accumulated.authors = accumulated.authors +: current.author )
// def safeToInt(s: String) : Int = Try(s.toInt).toOption.getOrElse(0)
// def safeToLong(s: String) : Long = Try(s.toLong).toOption.getOrElse(0)

val dfPaperAuthors = dfPaperAuthorAff.groupBy("paper").agg(collect_list($"author").as("authors"))
val papersAuthorsWithYears = dfPaperAuthors.join(dfsPapers, $"id" === $"paper")//.select("id", "year", "authors")//.withColumn("author",lit(0L))

val coauthorsByYear = papersAuthorsWithYears.select(explode($"authors").as("author"), $"year", $"authors")
val flatten = udf((xs: Seq[Seq[Long]]) => xs.flatten.distinct)
// Agregat
// val dfCoauthors = coauthorsByYear.groupBy("author", "year")
//         .agg(concat_ws(":", $"year",concat_ws(";", flatten(collect_list("authors")))).as("authors"))
//         .groupBy("author")
//         .agg(concat_ws("|", collect_list($"authors")).as("coauthors"))
val dfCoauthors = coauthorsByYear.groupBy("author", "year")
  .agg(flatten(collect_list("authors")).as("authors"))
  .select($"author", $"year", explode($"authors").as("coauthor"))
  .filter($"author" =!= $"coauthor") // drop duplicates


// COMMAND ----------

// val authorID = 396228672L // Traian Florin Serbanuta
// val fdfCoauthors = dfCoauthors.filter(row => row.getLong(0) == authorID)

// COMMAND ----------

// MAGIC %md
// MAGIC # Aggregate info
// MAGIC We will add for each author in a specific year :
// MAGIC   - the number of citations in that year
// MAGIC   - the number of publications in that year
// MAGIC   - the h-Index in that year

// COMMAND ----------

import org.apache.spark.sql.functions._

val minYear = 2000
val maxYear = 2018
// create a dataset with years from minRange to maxRange
val range : List[Int] = (minYear to maxYear).toList
val analysisRange = spark.createDataset(range).withColumnRenamed("value", "year")
// get how many citations got a paper in each year (we look only at citations happening before maxYear)
val citationsByYear = dfPaperReferences.join(dfsPapers, $"citing" === $"id").groupBy("year", "cited").count().filter($"year" <= maxYear)
  .cache
// add the first year in which a paper has been cited
val citedPapersMinYear = citationsByYear.groupBy("cited").agg(min($"year").as("minYear"))
// create a row for each cited paper and each of the years in our analysys range. 
// filter because we only keep years onward from the first year a paper has been cited 
val citedInRange = citedPapersMinYear.crossJoin(analysisRange).filter($"year" >= $"minYear").drop("minYear").withColumn("count", lit(0))
// add the counts we computed earlier to the corresponding years for each paper
// groupBy because we want to remove the entries of (cited, year) that appear in both dataframes that are unioned
val citationsByYearWithExtraRange = citationsByYear.unionByName(citedInRange).groupBy("cited", "year").agg(sum($"count").as("count"))
  .cache

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions. {sum, count}
// use a window to sum up aggregate citations in all the years
val yearCumulWindow = Window.partitionBy("cited").orderBy("year").rangeBetween(Window.unboundedPreceding, Window.currentRow )
val citationsByYearAgg = citationsByYearWithExtraRange.withColumn("aggregated", sum($"count").over(yearCumulWindow)).filter($"year" >= minYear)
  .cache



// COMMAND ----------

// udf for hIndex computation
val hIndex = udf(
    (aggr : Seq[Long]) => aggr.sortWith(_ > _).zipWithIndex.filter( { case (agg : Long, index : Int) => agg >= (index + 1) }).size
  );

// count how many papers wrote an author in each year
val dfPaperCounts = dfPaperAuthorAff.join(dfsPapers, $"paper" === $"id").groupBy("author", "year").count().withColumnRenamed("count","paperCount")
  .cache

// get a paper count for each author in each year in the analysis range
// perform same algorithm as above
val paperCountsMinYear = dfPaperCounts.groupBy("author").agg(min($"year").as("minYear"))

val papersInRange = paperCountsMinYear.crossJoin(analysisRange).filter($"year" >= $"minYear").drop("minYear").withColumn("paperCount", lit(0))

val dfPaperCountsWithExtraRange = dfPaperCounts.unionByName(papersInRange).groupBy("author", "year").agg(sum($"paperCount").as("paperCount"))

val yearPaperCumulWindow = Window.partitionBy("author").orderBy("year").rangeBetween(Window.unboundedPreceding, Window.currentRow )
val authorPapersByYearAgg = dfPaperCountsWithExtraRange
  .withColumn("aggregatedPaperCount", sum($"paperCount").over(yearPaperCumulWindow))
  .filter($"year" >= minYear)
  .cache

// COMMAND ----------


val authorsCitations = citationsByYearAgg.join(dfPaperAuthorAff, $"cited" === $"paper")
  .groupBy("author","year")
  .agg( sum($"count").as("citationCount")
  , sum($"aggregated").as("aggregatedCitationCount")
  , hIndex(collect_list($"aggregated")).as("hIndex"))
  .cache

// var dfAuthorsInfo = papersByYearAgg.join(dfAuthorsCitations, Seq("author", "year"), "left").na.fill(0, Seq("citationCount", "aggregatedCitationCount", "hIndex"))

// COMMAND ----------

val dfAuthorsInfo = authorPapersByYearAgg.as("AP")
  .join(authorsCitations.as("AC"), $"AP.author" === $"AC.author" && $"AP.year" === $"AC.year")
  .select($"AC.author".as("author"), $"AC.year".as("year"), $"AC.citationCount".as("citations"), $"AC.aggregatedCitationCount".as("aggregatedCitations")
          , $"AP.paperCount".as("papers"), $"AP.aggregatedPaperCount".as("agreggatedPapers"), $"AC.hIndex".as("hIndex")).cache
// .na.fill(0, Seq("noPapers", "noCitations", "noAggregatedCitations", "hIndex"))


// COMMAND ----------

// var dfAuthorsInfo = papersByYearAgg.join(authorsCitations, Seq("author", "year"), "full").na.fill(0, Seq("paperCount", "citationCount", "aggregatedCitationCount", "hIndex")).cache

// COMMAND ----------

// path variables
val adls = "magdls"     // adls name
val write_dir = "graph/result" //directory to which we write files
val wmag = "adl://" + adls + ".azuredatalakestore.net/" + write_dir
// fileCoauthors.write.partitionBy("author").format("com.databricks.spark.csv").save(wmag + "/output_1")
//fileCoauthors.coalesce(1).write.partitionBy("author").format("com.databricks.spark.csv").save(wmag + "/output1")
dfAuthorsInfo.write.format("com.databricks.spark.csv").save(wmag + "/AtributeAutori_2")