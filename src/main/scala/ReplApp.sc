import java.sql.Date

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

val spark = SparkSession.builder().appName("Iconic App").master("local[*]").getOrCreate()
val mag  = "/home/bob/gtd/Iconic/data/samples"


def getDataFrame(path: String, schema: StructType) : DataFrame = {
  spark.read
    .option("delimiter", "\t")
    .schema(schema)
    .csv(path)
}

/// PAPER
case class Paper(id : Long, rank: Int, doi: String, docType: String, title: String,
                 originalTitle: String, bookTitle: String, year: Int, date: Date,
                 publisher: String, journal: Long, conferenceSeries: Long, conferenceInstance: Long,
                 volume: String, issue: String, firstPage: String, lastPage: String,
                 references: Long,  citations: Long, estimatedCitations: Long, createdAt: Date)

/// AUTHOR
case class Author(id: Long, rank: Long, name: String, dname: String,
                  affiliation: Long, papers: Long, citations:Long, createdAt:Date)
/// FOS
case class Fos(id: Long, rank: Int, name: String, dname: String,
               mainType: String, level: Int, papers: Long, citations: Long, createdAt: Date)
/// PAPER AUTHOR AFFILIATION
case class PaperAuthorAff(paper: Long, author: Long, affiliation: Long, authorSequence: Int, originalAffiliation: String)
/// FOS CHILDREN
case class FosChildren(parent: Long,child: Long)
/// PAPER FOS
case class PaperFos(paper: Long, fos: Long, similarity: Double)
/// PAPER REFERENCE
case class PaperReference(citing: Long, cited: Long)

val paperSchema = ScalaReflection.schemaFor[Paper].dataType.asInstanceOf[StructType]
lazy val dfPapers = getDataFrame(mag + "/Papers.txt", paperSchema)

val authorSchema = ScalaReflection.schemaFor[Author].dataType.asInstanceOf[StructType]
lazy val dfAuthors = getDataFrame(mag + "/Authors.txt", authorSchema)

val fosSchema = ScalaReflection.schemaFor[Fos].dataType.asInstanceOf[StructType]
lazy val dfFos = getDataFrame(mag + "/FieldsOfStudy.txt", fosSchema)

val paperAuthorAffSchema = ScalaReflection.schemaFor[PaperAuthorAff].dataType.asInstanceOf[StructType]
lazy val dfPaperAuthorAff = getDataFrame(mag + "/PaperAuthorAffiliations.txt", paperAuthorAffSchema)

val fosChildrenSchema = ScalaReflection.schemaFor[FosChildren].dataType.asInstanceOf[StructType]
lazy val dfFosChildren = getDataFrame(mag + "/FieldOfStudyChildren.txt", fosChildrenSchema)

val paperFosSchema = ScalaReflection.schemaFor[PaperFos].dataType.asInstanceOf[StructType]
lazy val dfPaperFos = getDataFrame(mag + "/PaperFieldsOfStudy.txt", paperFosSchema)

val paperReferenceSchema = ScalaReflection.schemaFor[PaperReference].dataType.asInstanceOf[StructType]
lazy val dfPaperReferences = getDataFrame(mag + "/PaperReferences.txt", paperReferenceSchema)

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._

import org.apache.spark.sql.catalyst.encoders.RowEncoder
case class AuthorPaperFos(author : Long, paper : Long, affiliation : Long, fos : Long)
val schema = ScalaReflection.schemaFor[AuthorPaperFos].dataType.asInstanceOf[StructType]
val rowEncoder = RowEncoder(schema)

import org.apache.spark.sql.Row

case class Affiliation(id: Long, rank: Int, name: String, dname: String, grid: String,
                       page: String, wiki : String, papers : Long, citations : Long,
                       lat : Float, long: Long, createdAt: Date,
                       countryCode : String, country : String)

val affiliationSchema = ScalaReflection.schemaFor[Affiliation].dataType.asInstanceOf[StructType]
lazy val dfAff = getDataFrame(mag + "/Affiliations_FC.txt", affiliationSchema)

dfPaperAuthorAff.groupBy("author").count()












