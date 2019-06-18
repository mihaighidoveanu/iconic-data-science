package model

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import utils.SparkUtils

object MAG {

  // path for database files
  val mag = "/home/bob/gtd/Iconic/data/samples"

  val utils = new SparkUtils()

  private val paperSchema = ScalaReflection.schemaFor[Paper].dataType.asInstanceOf[StructType]
  lazy val dfPapers = utils.getDataFrame(mag + "/Papers.txt", paperSchema)

  private val authorSchema = ScalaReflection.schemaFor[Author].dataType.asInstanceOf[StructType]
  lazy val dfAuthors = utils.getDataFrame(mag + "/Authors.txt", authorSchema)

  private val fosSchema = ScalaReflection.schemaFor[Fos].dataType.asInstanceOf[StructType]
  lazy val dfFos = utils.getDataFrame(mag + "/FieldsOfStudy.txt", fosSchema)

  private val affiliationSchema = ScalaReflection.schemaFor[Affiliation].dataType.asInstanceOf[StructType]
  lazy val dfAff = utils.getDataFrame(mag + "/Affiliations_FC.txt", affiliationSchema)

  private val paperAuthorAffSchema = ScalaReflection.schemaFor[PaperAuthorAff].dataType.asInstanceOf[StructType]
  lazy val dfPaperAuthorAff = utils.getDataFrame(mag + "/PaperAuthorAffiliations.txt", paperAuthorAffSchema)

  private val fosChildrenSchema = ScalaReflection.schemaFor[FosChildren].dataType.asInstanceOf[StructType]
  lazy val dfFosChildren = utils.getDataFrame(mag + "/FieldOfStudyChildren.txt", fosChildrenSchema)

  private val paperFosSchema = ScalaReflection.schemaFor[PaperFos].dataType.asInstanceOf[StructType]
  lazy val dfPaperFos = utils.getDataFrame(mag + "/PaperFieldsOfStudy.txt", paperFosSchema)

  private val paperReferenceSchema = ScalaReflection.schemaFor[PaperReference].dataType.asInstanceOf[StructType]
  lazy val dfPaperReferences = utils.getDataFrame(mag + "/PaperReferences.txt", paperReferenceSchema)



}

