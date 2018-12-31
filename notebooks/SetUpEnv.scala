// Databricks notebook source
// MAGIC %md
// MAGIC ### Initial Setup
// MAGIC We access the Azure Data Lake Storage with Service to Service authentication. Spark uses an Active Directory app to read and write from the ADLS . Next we will set up the credentials for the app and the active directory of the owner account
// MAGIC We also initialize some global variables with the path to the ADLS. The variable will be given as an argument anywhere a function needs to read from the MAG data.

// COMMAND ----------

// dbutils.widgets.dropdown("debug", "true", Seq("true", "false"))

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
val mag_dir = "graph/2018-09-27"  // directory with the mag data
val smag_dir = "graph/samples" //directory with the sample mag data
val tmag_dir = "graph/test" //directory with the test mag data
val write_dir = "graph/result"
val mag = "adl://" + adls + ".azuredatalakestore.net/" + mag_dir
val smag = "adl://" + adls + ".azuredatalakestore.net/" + smag_dir
val tmag = "adl://" + adls + ".azuredatalakestore.net/" + tmag_dir
val wmag = "adl://" + adls + ".azuredatalakestore.net/" + write_dir

// COMMAND ----------

// MAGIC %md
// MAGIC ## Extracting data
// MAGIC Data is stored in text files on rows. Columns are separated by tab characters.
// MAGIC  + Construct case classes and get their schemas
// MAGIC  + Read files as csv with \t delimiter
// MAGIC  + Specify file schema according to its table's case class
// MAGIC 
// MAGIC We get a **DataFrame** for each table and can easily get a **Dataset** out of it by using *.as[CaseClass]*

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.Date
import org.apache.spark.sql.catalyst.ScalaReflection

def getDataFrame(path: String, schema: StructType) : DataFrame = {
  spark.read
    .option("delimiter", "\t")
    .schema(schema)
    .csv(path)
}
//define a case class to store information about each row of the textfile

/// PAPER
case class Paper(id : Long, rank: Int, doi: String, docType: String, title: String, 
  originalTitle: String, bookTitle: String, year: Int, date: Date, 
  publisher: String, journal: Long, conferenceSeries: Long, conferenceInstance: Long, 
  volume: String, issue: String, firstPage: String, lastPage: String, 
  references: Long,  citations: Long, estimatedCitations: Long, createdAt: Date)
var paperSchema = ScalaReflection.schemaFor[Paper].dataType.asInstanceOf[StructType]
val dfPapers = getDataFrame(mag + "/Papers.txt", paperSchema)

/// AUTHOR 
case class Author(id: Long, rank: Long, name: String, dname: String, 
                  affiliation: Long, papers: Long, citations:Long, createdAt:Date)
val authorSchema = ScalaReflection.schemaFor[Author].dataType.asInstanceOf[StructType]
val dfAuthors = getDataFrame(mag + "/Authors.txt", authorSchema)

/// FOS
case class Fos(id: Long, rank: Int, name: String, dname: String, 
              mainType: String, level: Int, papers: Long, citations: Long, createdAt: Date)
val fosSchema = ScalaReflection.schemaFor[Fos].dataType.asInstanceOf[StructType]
val dfFos = getDataFrame(mag + "/FieldsOfStudy.txt", fosSchema)

/// PAPER AUTHOR AFFILIATION
case class PaperAuthorAff(paper: Long, author: Long, affiliation: Long, authorSequence: Int, originalAffiliation: String)
val paperAuthorAffSchema = ScalaReflection.schemaFor[PaperAuthorAff].dataType.asInstanceOf[StructType]
val dfPaperAuthorAff = getDataFrame(mag + "/PaperAuthorAffiliations.txt", paperAuthorAffSchema)

/// FOS CHILDREN
case class FosChildren(parent: Long,child: Long)
val fosChildrenSchema = ScalaReflection.schemaFor[FosChildren].dataType.asInstanceOf[StructType]
val dfFosChildren = getDataFrame(mag + "/FieldOfStudyChildren.txt", fosChildrenSchema)

/// PAPER FOS
case class PaperFos(paper: Long, fos: Long, similarity: Double)
val paperFosSchema = ScalaReflection.schemaFor[PaperFos].dataType.asInstanceOf[StructType]
val dfPaperFos = getDataFrame(mag + "/PaperFieldsOfStudy.txt", paperFosSchema)

/// PAPER REFERENCE
case class PaperReference(citing: Long, cited: Long)
val paperReferenceSchema = ScalaReflection.schemaFor[PaperReference].dataType.asInstanceOf[StructType]
val dfPaperReferences = getDataFrame(mag + "/PaperReferences.txt", paperReferenceSchema)



// COMMAND ----------

// MAGIC %md
// MAGIC ## Register data as tables
// MAGIC if debugging, set only the test data. if not, register all the MAG data.

// COMMAND ----------

dfPapers.createOrReplaceGlobalTempView("papers")
dfAuthors.createOrReplaceGlobalTempView("authors")
dfPaperAuthorAff.createOrReplaceGlobalTempView("paa")
dfFos.createOrReplaceGlobalTempView("fos")
dfPaperFos.createOrReplaceGlobalTempView("pf")
dfFosChildren.createOrReplaceGlobalTempView("fc")
dfPaperReferences.createOrReplaceGlobalTempView("pr")



// COMMAND ----------

// used to return that everything is ok
dbutils.notebook.exit("OK")

// COMMAND ----------


val globalTempDb = spark.conf.get("spark.sql.globalTempDatabase")
val dfPapers = table(globalTempDb + ".papers")
val dfAuthors = table(globalTempDb + ".authors")
val dfFos = table(globalTempDb + ".fos")
val dfPaperAuthorAff = table(globalTempDb + ".paa")
val dfFosChildren = table(globalTempDb + ".fc")
val dfPaperFos = table(globalTempDb + ".pf")
val dfPaperReferences = table(globalTempDb + ".pr")
