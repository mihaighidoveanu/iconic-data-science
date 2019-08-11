package  model
import java.sql.DATE

/// Single Entities
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
/// AFFILIATION
case class Affiliation(id: Long, rank: Int, name: String, dname: String, grid: String,
                       page: String, wiki : String, papers : Long, citations : Long,
                       lat : Float, long: Long, createdAt: Date,
                       countryCode : String, country : String)
/// Relationships
/// PAPER AUTHOR AFFILIATION
case class PaperAuthorAff(paper: Long, author: Long, affiliation: Long, authorSequence: Int, originalAffiliation: String)
/// PAPER FOS
case class PaperFos(paper: Long, fos: Long, similarity: Double)
/// PAPER REFERENCE
case class PaperReference(citing: Long, cited: Long)
/// FOS CHILDREN
case class FosChildren(parent: Long,child: Long)




