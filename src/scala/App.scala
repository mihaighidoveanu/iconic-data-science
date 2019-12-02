import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import utils.SparkUtils
import org.apache.spark.sql.functions.{col, countDistinct}
import model.MAG

case class AuthorPaperFos(author : Long, paper : Long, affiliation : Long, fos : Long)

object Main {


  /*plan : get major field of study for each author
      - map each field of study to its parent
      = PAA join PF on PAA.paper -> {author, paper, fos}
      => map  -> {author, paper, root_fos}
      => group by (author, root_fos) -> {author, root_fos, [paper]}
      => aggregate -> {author, root_fos, no_papers}
    */
  def getRootFos(dfFos : DataFrame, fieldId : Long) : Long = {
    implicit val longEnc = Encoders.LONG
    val filtered = dfFos.filter(col("child") === fieldId).select(col("parent"))
    if(filtered.isEmpty){
      return fieldId
    }
    val parent = filtered.as(longEnc).take(1)(0)
    if(parent == fieldId){
      println(parent)
      parent
    }
    else{
      getRootFos(dfFos, parent)
    }
  }


  // author, fos, countryCode, country
  def getAuthorDetails() = {
    val schema = ScalaReflection.schemaFor[AuthorPaperFos].dataType.asInstanceOf[StructType]
    val rowEncoder = RowEncoder(schema)
    //test : test on cloud
    val authorDetails = MAG.dfPaperAuthorAff.join(MAG.dfPaperFos, "paper")
      .select("author", "paper","affiliation", "fos")
      .map(row => {
        val author = row.getLong(0)
        val paper = row.getLong(1)
        val aff = row.getLong(2)
        val fos = row.getLong(3)
        val root = getRootFos(MAG.dfFos, fos)
        Row(author, paper, aff, root)
      })(rowEncoder)
      .groupBy("author", "fos", "affiliation")
      .agg(countDistinct("paper"))
      .join(MAG.dfAff, col("affiliation") === col("id"))
      .select("author","fos","countryCode","country")

      authorDetails
  }

  def main(args: Array[String]): Unit = {
    val utils = new SparkUtils
    val spark = utils.spark
    import spark.implicits._

    var x = getAuthorDetails()



    /*
    = PAA join PF on PAA.paper -> {author, paper, fos, affiliation}
      => map  -> {author, paper, affiliation, root_fos}
      => group by (author, root_fos, affiliation) -> {author, root_fos, affiliation [paper]}
      => aggregate -> {author, affiliation, root_fos, no_papers}
      ----------------------
      => join Aff on Aff.affiliation -> {author, root_fos, no_papers, country}
     */

    /* Ce facem ?
    Ion, Fizica, Bucuresti
    Ion, Istorie, Paris
     */

    spark.stop()
  }
}