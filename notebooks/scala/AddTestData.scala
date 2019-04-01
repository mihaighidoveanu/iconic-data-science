// Databricks notebook source
import org.apache.spark.sql.types._
import java.sql.Date
//define a case class to store information about each row of the textfile

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

// COMMAND ----------

//create test database
val r = scala.util.Random
val noPapers = 1000
val noAuthors = 2000
val noFields = 200
var testPapers = Seq() : Seq[Paper]
for(i <- 1 to noPapers){
  testPapers = testPapers ++ Seq(Paper(i,i,"","","paper " + i,"","",1990 + i % 2,Date.valueOf("1997-01-22"),
                                       "",1,1,1,"","","","",r.nextInt(5),r.nextInt(3),r.nextInt(3),Date.valueOf("1997-01-22")))
}    
var testPapers1 = Seq() : Seq[Paper]
for(i <- 0 to 3){
  testPapers1 = testPapers1 ++ Seq(Paper(i,i,"","","paper " + i,"","",1993 - i,Date.valueOf("1997-01-22"),
                                       "",1,1,1,"","","","",r.nextInt(5),r.nextInt(3),r.nextInt(3),Date.valueOf("1997-01-22")))
}
for(i <- 4 to 7){
  testPapers1 = testPapers1 ++ Seq(Paper(i,i,"","","paper " + i,"","",2000 + i,Date.valueOf("1997-01-22"),
                                       "",1,1,1,"","","","",r.nextInt(5),r.nextInt(3),r.nextInt(3),Date.valueOf("1997-01-22")))
}
// add one extra paper in the same year
 testPapers1 = testPapers1 ++ Seq(Paper(8,8,"","","paper " + 8,"","",2007,Date.valueOf("1997-01-22"),
                                       "",1,1,1,"","","","",r.nextInt(5),r.nextInt(3),r.nextInt(3),Date.valueOf("1997-01-22")))
testPapers1 = testPapers1 ++ Seq(Paper(9,9,"","","paper " + 9,"","",2007,Date.valueOf("1997-01-22"),
                                       "",1,1,1,"","","","",r.nextInt(5),r.nextInt(3),r.nextInt(3),Date.valueOf("1997-01-22")))
var testAuthors = Seq() : Seq[Author]
for(i <- 1 to noAuthors){
  testAuthors = testAuthors ++ Seq(Author(i,i,"Author" + i, "Author" + i,0,r.nextInt(2),r.nextInt(2),Date.valueOf("1997-01-22")))
}
var testPaa = Seq() : Seq[PaperAuthorAff]
for(i <- 1 to noAuthors){
  testPaa = testPaa ++ Seq(PaperAuthorAff(r.nextInt(noPapers),i,0,0,""))
}
var testPaa1 = Seq(PaperAuthorAff(0,1,0,0,""), PaperAuthorAff(0,2,0,0,""), PaperAuthorAff(0,3,0,0,""),
                   PaperAuthorAff(1,1,0,0,""), PaperAuthorAff(1,2,0,0,""),
                   PaperAuthorAff(2,2,0,0,""), PaperAuthorAff(2,3,0,0,""), PaperAuthorAff(2,4,0,0,""),
                   PaperAuthorAff(3,2,0,0,""), PaperAuthorAff(3,5,0,0,""), 
                   PaperAuthorAff(4,1,0,0,""), PaperAuthorAff(4,2,0,0,""), PaperAuthorAff(4,3,0,0,""), 
                   PaperAuthorAff(5,4,0,0,""), PaperAuthorAff(5,5,0,0,""), 
                   PaperAuthorAff(6,6,0,0,""), PaperAuthorAff(6,7,0,0,""),
                   PaperAuthorAff(7,6,0,0,""), PaperAuthorAff(7,7,0,0,""),
                   PaperAuthorAff(8,5,0,0,""),
                   PaperAuthorAff(9,7,0,0,"") )
var testPaa2 = Seq(PaperAuthorAff(0,1,0,0,""),PaperAuthorAff(0,2,0,0,""),PaperAuthorAff(0,3,0,0,""),
                 PaperAuthorAff(1,2,0,0,""),PaperAuthorAff(1,3,0,0,""),PaperAuthorAff(1,4,0,0,""))
var testPf = Seq() : Seq[PaperFos]
for(i <- 1 to noPapers){
  testPf = testPf ++ Seq(PaperFos(i,r.nextInt(noFields),0))
}
var testPr = Seq() : Seq[PaperReference]
for(i <- 1 to noPapers){
  testPr = testPr ++ Seq(PaperReference(r.nextInt(noPapers), i))
}
var testPr1 = Seq(PaperReference(0, 1), PaperReference(0, 3),
                 PaperReference(1, 2), PaperReference(1, 3),
                 PaperReference(2, 3),
                  PaperReference(6, 5),
                 PaperReference(7, 1), PaperReference(7, 3), 
                // autocitare
                 PaperReference(7, 6),
                // extra line for citin two papers of the same author in a year
                 PaperReference(7, 5), PaperReference(7, 8),
                // extra line for citing same paper twice in the same year
                 PaperReference(9,8))
val tfPapers = spark.createDataset(testPapers1)
val tfAuthors = spark.createDataset(testAuthors)
val tfPaa = spark.createDataset(testPaa1)
val tfPf = spark.createDataset(testPf)
val tfPr = spark.createDataset(testPr1)


// COMMAND ----------

tfPapers.createOrReplaceGlobalTempView("papers")
tfAuthors.createOrReplaceGlobalTempView("authors")
tfPaa.createOrReplaceGlobalTempView("paa")
tfPf.createOrReplaceGlobalTempView("pf")
tfPr.createOrReplaceGlobalTempView("pr")

// COMMAND ----------

// used to return that everything is ok
dbutils.notebook.exit("OK")