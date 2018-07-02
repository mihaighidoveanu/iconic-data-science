// Databricks notebook source
// MAGIC %md 
// MAGIC # This kind of works, but is not used at the moment
// MAGIC This didn't prove necessary for the moment. It proved better in response times to create test / dummy data for the development/testing phase.

// COMMAND ----------

// MAGIC %md
// MAGIC We will truncate files in MAG for the development/testing phase. Thus, we optimize the cost and speed of the phase. 
// MAGIC The algortihms used on the truncated files should be able to handle the big files with the same methodology.
// MAGIC We connect to the Azure Data Lake Store and then follow an algorithm to get the desired data

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
val mag_dir = "graph/2018-05-07"  // directory with the mag data
val mag_dir_samples = "graph/samples"
val mag = "adl://" + adls + ".azuredatalakestore.net/" + mag_dir
val mag_samples = "adl://" + adls + ".azuredatalakestore.net/" + mag_dir_samples
val samples_mount = "/mnt/samples"

// DBFS-to-ADLS mount point
val configs = Map(
  "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
  "dfs.adls.oauth2.client.id" -> client_id,
  "dfs.adls.oauth2.credential" -> client_secret,
  "dfs.adls.oauth2.refresh.url" -> ("https://login.microsoftonline.com/" + tenant_id + "/oauth2/token"))

// clear up
dbutils.fs.unmount(samples_mount)
// mount the data lake 
dbutils.fs.mount(
  source = "adl://" + adls + ".azuredatalakestore.net/" + mag_dir_samples,
  mountPoint = "/mnt/samples",
  extraConfigs = configs)



// COMMAND ----------

import org.apache.spark.sql.Dataset
// We will read lines from different files and store them in a map
// We keep the map to be able to write all the samples at once
val samples = scala.collection.mutable.Map[String, Dataset[String]]() 
val noPapers = 1000
val allPapers = spark.read.textFile(mag + "/Papers.txt")
samples("Papers") = spark.createDataset(allPapers.take(noPapers))
//it would be nice to random sample the text files, instead of getting the first ones // val samplePapers = allPapers.sample(false,0.001) 
// get paperIds from samplePapers
val paperIds = samples("Papers").
  map(_.split("\t",-1)).
  map(words => words(0)).
  withColumnRenamed("value","id").
  cache()

// COMMAND ----------

import org.apache.spark.sql.DataFrame
def joinFileById(dfWithId : DataFrame, path: String) : DataFrame = {
  spark.read.textFile(path).
  map(_.split("\t",-1)).
  withColumnRenamed("value","words").
  join(dfWithId, $"id" === $"words"(0))
}
def getSampleById(dfWithId : DataFrame, path: String) : Dataset[String] = {
  joinFileById(dfWithId, path).
  select($"words").
  map(_.mkString("\t"))
}

// use these paperIds to get corresponding authorIds 
val authorIds = joinFileById(paperIds,mag + "/PaperAuthorAffiliations.txt").
  select($"words"(1).name("id"), $"words").
  cache()

// form the sample with the lines from where we got the AuthorIds
samples("PaperAuthorAffiliations") = authorIds.
  select($"words").
  map(_.mkString("\t"))

// sample the file describing Authors using the authorIds we got
// we will drop "words" column of authorIds when joining, to avoid ambiguity with the next dataframe
samples("Authors") = getSampleById(authorIds.drop("words"),mag + "/Authors.txt")

// COMMAND ----------

// use these paperIds to get corresponding fields of Study Ids 
val fosIds = joinFileById(paperIds, mag + "/PaperFieldsOfStudy.txt").
  select($"words"(1).name("id"),$"words").
  cache()
// form the sample with the lines from where we got fosIds
samples("PaperFieldsOfStudy") = fosIds.
  select($"words").
  map(_.mkString("\t"))
//use field of study id to also get the ids of their parents and children
// we will drop "words" column of fosIds when joining, to avoid ambiguity with the next dataframe
val fosIdsFamilly = spark.read.textFile(mag + "/FieldOfStudyChildren.txt").
  map(_.split("\t", -1)).
  withColumnRenamed("value","words").
  join(fosIds.drop("words"),
       $"id" === $"words"(0) || $"id" === $"words"(1)).
  dropDuplicates().
  select($"words"(0).name("idParent"), $"words"(1).name("idChild"),$"words").
  cache()
// form the sample with the lines from where we got fosIds
samples("FieldsOfStudyChildren") = fosIdsFamilly.
  select($"words").
  map(_.mkString("\t"))
// sample the file describing Fields using the fosIdsFamilly we got
// we will drop "words" column of fosIds when joining, to avoid ambiguity with the next dataframe
//note : the first fosIds are contained as child or parent in fosIdsFamilly
samples("FieldsOfStudy") = spark.read.textFile(mag + "/FieldsOfStudy.txt").
  map(_.split("\t", -1)).
  withColumnRenamed("value","words").
  join(fosIdsFamilly.drop("words"),
       $"idParent" === $"words"(0) || $"idChild" === $"words"(0)).
  dropDuplicates().
  select($"words").
  map(_.mkString("\t"))

// COMMAND ----------

//sample some auxiliary files regarding the paperIds

samples("PaperCitationContexts") = getSampleById(paperIds, mag + "/PaperCitationContexts.txt")
samples("PaperLanguages") = getSampleById(paperIds, mag + "/PaperLanguages.txt")
samples("PaperRecommendations") = getSampleById(paperIds, mag + "/PaperRecommendations.txt")
samples("PaperReferences") = getSampleById(paperIds, mag + "/PaperReferences.txt")
samples("PaperUrls") = getSampleById(paperIds, mag + "/PaperUrls.txt")

// COMMAND ----------

//write the samples to file
samples.foreach {
  case (fileName, dataset) => dataset.rdd.saveAsTextFile(samples_mount + "/" + fileName + ".txt")
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Interactive work from here on

// COMMAND ----------

// samples("FieldsOfStudy").rdd.saveAsTextFile(samples_mount + "/FieldsOfStudy.txt")

// COMMAND ----------

