package model

import java.sql.Date

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import utils.SparkUtils


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




class MAG {



  var paperSchema = ScalaReflection.schemaFor[Paper].dataType.asInstanceOf[StructType]
  var mag = ""
  //todo : make dataframes available to the outside world
  //todo : make dataframes instantiation lazy

  def init(utils: SparkUtils) = {

    val paperSchema = ScalaReflection.schemaFor[Paper].dataType.asInstanceOf[StructType]
    lazy val dfPapers = utils.getDataFrame(mag + "/Papers.txt", paperSchema)

    val authorSchema = ScalaReflection.schemaFor[Author].dataType.asInstanceOf[StructType]
    lazy val dfAuthors = utils.getDataFrame(mag + "/Authors.txt", authorSchema)

    val fosSchema = ScalaReflection.schemaFor[Fos].dataType.asInstanceOf[StructType]
    lazy val dfFos = utils.getDataFrame(mag + "/FieldsOfStudy.txt", fosSchema)

    val paperAuthorAffSchema = ScalaReflection.schemaFor[PaperAuthorAff].dataType.asInstanceOf[StructType]
    lazy val dfPaperAuthorAff = utils.getDataFrame(mag + "/PaperAuthorAffiliations.txt", paperAuthorAffSchema)

    val fosChildrenSchema = ScalaReflection.schemaFor[FosChildren].dataType.asInstanceOf[StructType]
    lazy val dfFosChildren = utils.getDataFrame(mag + "/FieldOfStudyChildren.txt", fosChildrenSchema)

    val paperFosSchema = ScalaReflection.schemaFor[PaperFos].dataType.asInstanceOf[StructType]
    lazy val dfPaperFos = utils.getDataFrame(mag + "/PaperFieldsOfStudy.txt", paperFosSchema)

    val paperReferenceSchema = ScalaReflection.schemaFor[PaperReference].dataType.asInstanceOf[StructType]
    lazy val dfPaperReferences = utils.getDataFrame(mag + "/PaperReferences.txt", paperReferenceSchema)

  }


}

