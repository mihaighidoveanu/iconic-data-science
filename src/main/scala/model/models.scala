import java.sql.Date

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

//todo : make case classes available to the outside world

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


