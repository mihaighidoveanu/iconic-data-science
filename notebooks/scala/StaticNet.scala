// Databricks notebook source
// MAGIC %md
// MAGIC # This works with no problems. 
// MAGIC It's best to experience using the net here, for starters. Currently, the personalNets are taken smaller than they really are because of time costs,  but this can be set up according to one's needs by changing variables like 'depth' in the code.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Initial Setup
// MAGIC We access the Azure Data Lake Storage with Service to Service authentication. Spark uses an Active Directory app to read and write from the ADLS . Next we will set up the credentials for the app and the active directory of the owner account
// MAGIC We also initialize some global variables with the path to the ADLS. The variable will be given as an argument anywhere a function needs to read from the MAG data.

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
val mag_dir = "graph/2018-06-21"  // directory with the mag data
val smag_dir = "graph/samples" //directory with the sample mag data
val tmag_dir = "graph/test" //directory with the test mag data
val mag = "adl://" + adls + ".azuredatalakestore.net/" + mag_dir
val smag = "adl://" + adls + ".azuredatalakestore.net/" + smag_dir
val tmag = "adl://" + adls + ".azuredatalakestore.net/" + tmag_dir

// COMMAND ----------

// MAGIC %md
// MAGIC ## Extracting data
// MAGIC Data is stored in text files on rows. Columns are separated by tab characters

// COMMAND ----------

import org.apache.spark.sql._

def getDataFrame(path: String) : Dataset[Array[String]] = {
  spark.read.textFile(path).
  map(line => line.split("\t", -1)).
  map(words => words.map(word => word.trim))
}

// read text file into an Dataset[String] and split the underlying string into an array of words (semanticly columns)
val sourcePapers = getDataFrame(mag + "/Papers.txt")
val sourceAuthors = getDataFrame(mag + "/Authors.txt")
val sourceFos = getDataFrame(mag + "/FieldsOfStudy.txt")
val sourceFosChildren = getDataFrame(mag + "/FieldOfStudyChildren.txt")
val sourcePaperAuthorAff = getDataFrame(mag + "/PaperAuthorAffiliations.txt")
val sourcePaperFos = getDataFrame(mag + "/PaperFieldsOfStudy.txt")
val sourcePaperReferences = getDataFrame(mag + "/PaperFieldsOfStudy.txt")



// COMMAND ----------

// MAGIC %md
// MAGIC ## Formatting the data
// MAGIC We know the data schemes from [here](https://microsoftdocs.github.io/MAG/Mag-ADLS-Schema)
// MAGIC We convert each row to a case class and transform the *RDD* of classes to a **Dataset**.
// MAGIC Now we can analyze and manipulate the data more efficiently, in the form of a DS.

// COMMAND ----------

import scala.util.Try
import java.text.{SimpleDateFormat, ParsePosition}
import java.sql.Date
// define conversion utilities to cast strings in file to the appropiate values
val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
def safeToInt(s: String) : Int = Try(s.toInt).toOption.getOrElse(0)
def safeToLong(s: String) : Long = Try(s.toLong).toOption.getOrElse(0)
def safeToDouble(s: String) : Double = Try(s.toDouble).toOption.getOrElse(0)

//define a case class to store information about each row of the textfile
case class Paper(id : Long, rank: Int, docType:String, doi: String, title: String, 
  original: String, book: String, year: Int, date: Date, 
  publisher: String, journal: Long, conferenceSeries: Long, conferenceInstance: Long, 
  volume: String, issue: String, firstPage: String, lastPage: String, 
  references: Long,  citations: Long, estimatedCitations: Long, createdAt: Date)

// finish transformations and get the resulted DataFrame
val dfPapers = sourcePapers.
  map(atr => Paper(safeToLong(atr(0)), safeToInt(atr(1)), atr(2), atr(3),
                   atr(4), atr(5), atr(6), safeToInt(atr(7)),
                   Date.valueOf(atr(8)), atr(9), safeToLong(atr(10)), safeToLong(atr(11)),
                   safeToLong(atr(12)), atr(13), atr(14), atr(15),
                   atr(16), safeToLong(atr(17)), safeToLong(atr(18)), safeToLong(atr(19)),
                   Date.valueOf(atr(20)))).
  cache()

case class Author(id: Long, rank: Long, name: String, dname: String, 
                  affiliation: Long, papers: Long, citations:Long, createdAt:Date)
val dfAuthors = sourceAuthors.
  map(a => Author(safeToLong(a(0)), safeToInt(a(1)), a(2), a(3),
                 safeToLong(a(4)), safeToLong(a(5)), safeToLong(a(6)), Date.valueOf(a(7)))).
  cache()

case class Fos(id: Long, rank: Int, name: String, dname: String, 
              mainType: String, level: Int, papers: Long, citations: Long, createdAt: Date)
val dfFos = sourceFos.
  map(a => Fos(safeToLong(a(0)), safeToInt(a(1)), a(2), a(3), 
               a(4), safeToInt(a(5)), safeToLong(a(6)), safeToLong(a(7)), Date.valueOf(a(8)))).
  cache()
case class FosChildren(parent: Long,child: Long)
val dfFosChildren = sourceFosChildren.
  map(a => FosChildren(safeToLong(a(0)), safeToLong(a(1)))).
  cache()
case class PaperAuthorAff(paper: Long,author: Long, affiliation: Long, authorSequence: Int)
val dfPaperAuthorAff = sourcePaperAuthorAff.
  map(a => PaperAuthorAff(safeToLong(a(0)), safeToLong(a(1)), safeToLong(a(2)), safeToInt(a(3)))).
  cache()
case class PaperFos(paper: Long, fos: Long, similarity: Double)
val dfPaperFos = sourcePaperFos.
  map(a => PaperFos(safeToLong(a(0)), safeToLong(a(1)), safeToDouble(a(2)))).
  cache()
case class PaperReference(id: Long, reference: Long)
val dfPaperReferences = sourcePaperReferences.
  map(a => PaperReference(safeToLong(a(0)), safeToLong(a(1)))).
  cache()




// COMMAND ----------

// MAGIC %md
// MAGIC ## Adding test data

// COMMAND ----------

//create test database
val r = scala.util.Random
val noPapers = 1000
val noAuthors = 2000
val noFields = 200
var testPapers = Seq() : Seq[Paper]
for(i <- 1 to noPapers){
  testPapers = testPapers ++ Seq(Paper(i,i,"","","paper " + i,"","",1990 + i / 100,Date.valueOf("1997-01-22"),
                                       "",1,1,1,"","","","",r.nextInt(5),r.nextInt(3),r.nextInt(3),Date.valueOf("1997-01-22")))
}               
var testAuthors = Seq() : Seq[Author]
for(i <- 1 to noAuthors){
  testAuthors = testAuthors ++ Seq(Author(i,i,"Author" + i, "Author" + i,0,r.nextInt(2),r.nextInt(2),Date.valueOf("1997-01-22")))
}
// var testPaa = Seq() : Seq[PaperAuthorAff]
// for(i <- 1 to noAuthors){
//   testPaa = testPaa ++ Seq(PaperAuthorAff(r.nextInt(1000),i,0,0))
// }
var testPaa1 = Seq(PaperAuthorAff(0,1,0,0),PaperAuthorAff(0,2,0,0),PaperAuthorAff(0,3,0,0),
                 PaperAuthorAff(1,1,0,0),PaperAuthorAff(1,2,0,0),
                 PaperAuthorAff(2,2,0,0),PaperAuthorAff(2,4,0,0),PaperAuthorAff(2,3,0,0),
                 PaperAuthorAff(3,2,0,0),PaperAuthorAff(3,4,0,0),PaperAuthorAff(3,5,0,0))
var testPaa2 = Seq(PaperAuthorAff(0,1,0,0),PaperAuthorAff(0,2,0,0),PaperAuthorAff(0,3,0,0),
                 PaperAuthorAff(1,2,0,0),PaperAuthorAff(1,3,0,0),PaperAuthorAff(1,4,0,0))
var testPf = Seq() : Seq[PaperFos]
for(i <- 1 to noPapers){
  testPf = testPf ++ Seq(PaperFos(i,r.nextInt(noFields),0))
}
val tfPapers = spark.createDataset(testPapers)
val tfAuthors = spark.createDataset(testAuthors)
val tfPaa = spark.createDataset(testPaa2)
val tfPf = spark.createDataset(testPf)


// COMMAND ----------

// MAGIC %md
// MAGIC ## Register data as tables
// MAGIC if debugging, set only the test data. if not, register all the MAG data.

// COMMAND ----------

val debug = false
if(debug == true)
{
  tfPapers.createOrReplaceTempView("papers")
  tfAuthors.createOrReplaceTempView("authors")
  tfPaa.createOrReplaceTempView("paa")
  tfPf.createOrReplaceTempView("pf")
}
else
{
  dfPapers.createOrReplaceTempView("papers")
  dfAuthors.createOrReplaceTempView("authors")
  dfPaperAuthorAff.createOrReplaceTempView("paa")
  dfPaperFos.createOrReplaceTempView("pf")
  dfFosChildren.createOrReplaceTempView("fc")
  dfPaperReferences.createOrReplaceTempView("pr")
  //cache the tables
  spark.catalog.cacheTable("papers")
  spark.catalog.cacheTable("authors")
  spark.catalog.cacheTable("paa")
  
}

case class Link(src:Long, dst: Long, papersTogether: Long)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC # Creating the academic network
// MAGIC The academic network is a graphframe, with authors as vertexes and edges representing wether or not two authors collaborated on papers. Edges also contain information about how many papers did two authors collaborate on.
// MAGIC To create this academic network, we need these primary functions :
// MAGIC   - authors of paper
// MAGIC   - papers of author
// MAGIC   
// MAGIC We use some temporary views to make the datasets we read from files available globally to these functions

// COMMAND ----------

// MAGIC %md
// MAGIC # Static Network

// COMMAND ----------

val log = false
val count = true
val source = 2779274734L
2062021687

def computePapersTogether(src: Long, dst: Long) : Long = {
  val dfPaa = spark.table("paa").as[PaperAuthorAff]
  val srcPapers = dfPaa.filter(_.author == src).select($"paper".as("p"))
  val together = dfPaa.join(srcPapers,$"paper" === $"p").drop("p").as[PaperAuthorAff].filter(_.author == dst).count
  together
}
// authors of paper
def authorIdsOfPaper(thePaper: Long) : Array[Long] = {
  val dfPaa = spark.table("paa").as[PaperAuthorAff]
  val authorIds = dfPaa.filter($"paper" === thePaper).select($"author").as[Long].collect
  authorIds
}
def authorsOfPaper(thePaper: Long) : Array[Author] = {
  val dfPaa = spark.table("paa").as[PaperAuthorAff]
  val dfAuthors = spark.table("authors").as[Author]
  val authorIds = dfPaa.filter($"paper" === thePaper).select($"author").as[Long].collect
  val bcAuthorIds = sc.broadcast(authorIds)
  dfAuthors.filter(author => bcAuthorIds.value.contains(author.id)).collect
}
// papers of author
def paperIdsOfAuthor(theAuthor: Long) : Array[Long] = {
  val dfPaa = spark.table("paa").as[PaperAuthorAff]
  val paperIds = dfPaa.filter($"author" === theAuthor).select($"paper").as[Long].collect
  paperIds
}
def papersOfAuthor(theAuthor: Long) : Array[Paper] = {
  val dfPaa = spark.table("paa").as[PaperAuthorAff]
  val dfPapers = spark.table("papers").as[Paper]
  val paperIds = dfPaa.filter(_.author == theAuthor).select($"paper").as[Long].collect
  val bcPaperIds = sc.broadcast(paperIds)
  dfPapers.filter(paper => bcPaperIds.value.contains(paper.id)).collect
}

def getAuthors(ids : Seq[Long]) : Array[Author] = {
  val dfAuthors = spark.table("authors").as[Author]
  val bcIds = sc.broadcast(ids)
  dfAuthors.filter(author => bcIds.value.contains(author.id)).collect
}

def rootOfFos(theFos: Long) : Long = {
  val dfFc = spark.table("fc").as[FosChildren].collect()
  var child = theFos
  while(dfFc.filter(_.child == child).size > 0) {
    child = dfFc(0).parent
  }
  dfFc(0).parent
}

def authorsOfFos(theFos: Long) : Array[Author] = {
  val dfPf = spark.table("pf").as[PaperFos]
  val dfAuthors = spark.table("authors").as[Author]
  val dfPaa = spark.table("paa").as[PaperAuthorAff]
  val paperIds = dfPf.filter($"fos" === theFos).select($"paper")
  val authorIds = dfPaa.
    join(paperIds, paperIds("paper") === dfPaa("paper")).
    select(dfPaa("author"))
  dfAuthors.join(authorIds,$"author" === $"id").drop("author").as[Author].collect()
}

def combinations(xs: Seq[Long]) = for(i <- 0 to xs.size - 2;j <- i + 1 to xs.size - 1) yield(xs(i),xs(j))
def cartesian(xs : Seq[Long], ys : Seq[Long])= for(x <- xs; y <- ys) yield (x,y)
def doubleTuple(tuple : Tuple2[Long,Long]) : Seq[Tuple2[Long,Long]] = Seq( tuple, (tuple._2,tuple._1) )
def tupleToLink(tuple : Tuple2[Long,Long]) : Link = Link(tuple._1,tuple._2,1)
def linkToTuple(link : Link) : Tuple2[Long,Long] = (link.src,link.dst)

// create a graph with coauthors as vertexes and coauthorship links as edges

// def personalNet(source: Long ) : (Dataset[Author], Dataset[Link]) = {
  // include the source Author
  val dfAuthors = spark.table("authors").as[Author].cache
  var allCoa = Seq(source) : Seq[Long]
  var allLinks = Seq() : Seq[Link]
  var allPapers = Seq() : Seq[Long]
  var authorsToAnalize = Seq( (source,1) ) : Seq[(Long,Int)]  // sequence of authors to analize and the level at which they were found
  var currentAuthor = 0L
  val depth = 6 // depth of network
  val papersToLookAt  = 2 // number of papers to analyze
  while(authorsToAnalize.size > 0)
  {
    // set the author to analize
    currentAuthor = authorsToAnalize(0)._1
    val currentLevel = authorsToAnalize(0)._2
    // remove the currently analized author
    authorsToAnalize = authorsToAnalize.filter(pair => pair._1 != currentAuthor)
    var newCoa = Seq() : Seq[Long]
    val authorsPapers = paperIdsOfAuthor(currentAuthor)
    // get the newPapers
    val newPapers = authorsPapers.diff(allPapers)
    // add the newly found papers to all papers    
    allPapers = allPapers ++ newPapers

    if(count) { // loading bar
      println("==============")
      println("Remaining : "+ authorsToAnalize.size)
      println("Currently : "+ currentAuthor)
      println("Papers    : "+ authorsPapers.size)
    }    
    

    /*  *de optimizat* 
    *    Acest foreach de mai jos acționează algoritmul pentru fiecare publicație a unui autor, însă are loc secvențial, pe nodul driver.
    *    Cred că ar putea fi executat paralel pe cluster, dacă foreach-ul ar avea loc pe o colecție paralizată cum e Dataset. Doar că Spark nu permite 
    * folosirea unor altor Dataset-uri în funcțiile aplicate unui Dataset, caz în care el aruncă la runtime o excepție criptică NullPointerException.
    *    Asta înseamnă că trebuie să scoatem codul care folosește Dataset-uri în afara foreach-ului.
    *    Mai jos, o versiune a ceea ce am spus este implementata, dar randurile în cauză sunt comentate, deoarece
    * mai apare încă o eroare criptică (ClassNotFound), pe care încă nu am depistat-o
    *    Dacă doriți să încercați, decomentați liniile marcate cu 'optim' și comentați-o pe cea cu 'neoptim'
    */ 
    //     val paPairs = newPapers.map(paper => (paper,authorIdsOfPaper(paper))) //optim
    //     val dsPaPairs = spark.createDataset(paPairs) //optim
    newPapers.take(papersToLookAt).foreach(paper => { //neoptim
//     dsPaPairs.foreach(pair => { //optim
//       val paper = pair._1 //optim
//       val authors = pair._2 //optim
      if(log) { println("========= Paper " + paper + " ======= Source " + currentAuthor + " =====") }
      // get the coauthors of each paper 
      val authors = authorIdsOfPaper(paper)
      val coauthors = authors.filter(_ != currentAuthor)
      // get the already discovered links and coauthors, and update their links
      // it is important to check for old links before adding new ones, because finding the olds will be more difficult afterwards
      val oldCoa = coauthors.intersect(allCoa)
      if(oldCoa.size > 0) //if there were priorly discovered coauthors
      { 
        val oldAuthorsToLink = oldCoa ++ Seq(currentAuthor)        
        // update links for all combinations of already discovered authors
        val oldLinks = allLinks.filter(link =>  oldAuthorsToLink.contains(link.src) && oldAuthorsToLink.contains(link.dst))
        // increment the paper count for the already discovered links
        val updatedLinks = oldLinks.map(l => Link(l.src,l.dst,l.papersTogether + 1))
        allLinks = allLinks.diff(oldLinks).union(updatedLinks)
        if(log) {
          println("OldCoa: ")
          oldCoa.foreach(l => print(l + " "))
          println
        }
      }     
      // get the new coauthors and add them to the network, with corresponding links
      newCoa = coauthors.diff(allCoa)
      if(newCoa.size > 0 ) //if there are any new coauthors
      {
        // add the new coauthors
        allCoa = (allCoa ++ newCoa)
        // links are created between each of the new coauthors with the other new ones, hence combinations
        // currentAuthor should be also linked with all the new coauthors
        val newAuthorsToLink = newCoa ++ Seq(currentAuthor)
        // links are created in both directions, hence doubleTuple
        val newLinks = combinations(newAuthorsToLink).flatMap(doubleTuple).map(tupleToLink)
        // and add them by union 
        allLinks = allLinks.union(newLinks)
        // if we didn't exceed the desired network depth, add the new coauthors to analization
        
        if(currentLevel < depth) { 
          val newCoaWithLevel = newCoa.map(c => (c,currentLevel + 1))
          authorsToAnalize = authorsToAnalize ++ newCoaWithLevel 
        }
        if(log) {
          println("NewCoa: ")
          newCoa.foreach(l => print(l + " "))
          println
//           println("Links : ")
//           newLinks.foreach(l => println(l + " "))
        }
      }
      // create links between each of the old coauthors and each of the new ones, hence cartesian
      if(newCoa.size > 0 && oldCoa.size > 0) // if there are both old and new ones
      {
        val newAuthorsToLink = newCoa
        val oldAuthorsToLink = oldCoa
        val mixLinks = cartesian(newAuthorsToLink,oldAuthorsToLink).flatMap(doubleTuple).map(tupleToLink)
        // add the new links by union
        allLinks = allLinks.union(mixLinks)
        if(log) {
//           println("MixLinks : ")
//           mixLinks.foreach(l => println(l + " "))
        }
      }
    })
  }
//   // return the results
  val vertices = getAuthors(allCoa)
  val dfVertex = spark.createDataset(vertices).cache()
  val dfLinks = spark.createDataset(allLinks).cache()
//   (dfVertex, dfLinks)
// }

// COMMAND ----------

dfVertex.collect

// COMMAND ----------

dfVertex.write.parquet(tmag + "/Vertex5")
dfLinks.write.parquet(tmag + "/Links5")


// COMMAND ----------

val a = dfAuthors.filter(_.name.contains("alin stefanescu"))
a.collect.take(100)

// COMMAND ----------

val ps = dfPaperAuthorAff.filter(_.author == 1992868240L).map(_.paper).collect
val pInfo = dfPapers.filter(p => ps.contains(p.id)).cache
pInfo.map(_.title).collect.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Preparing for vizualization

// COMMAND ----------

import org.apache.spark.sql.Dataset
def toGexf(net : (Dataset[Author],Dataset[Link]) ) : String = {
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
    "<gexf xmlns=\"https://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
    "\t<graph mode=\"static\" defaultedgetype=\"directed\">\n" +
    "\t\t<nodes>\n" +
    net._1.map(v => 
    "\t\t\t<node id=\"" + v.id + "\" label=\"" + v.dname + "\" />\n").collect.mkString +
    "\t\t</nodes>\n" +
    "\t\t<edges>\n" +
    net._2.map(e => 
    "\t\t\t<edge source=\"" + e.src + "\" target=\"" + e.dst + "\" label=\"" + e.papersTogether + "\" />\n").collect.mkString +
    "\t\t</edges>\n" +
    "\t</graph>\n" +
    "</gexf>"
}

// COMMAND ----------

// val dfVertex = spark.read.parquet(ssmag + "/Vertex2_" + index).as[Author]
// val dfLinks = spark.read.parquet(ssmag + "/Links2_" + index).as[Link]
val net = (dfVertex,dfLinks)
val graph = toGexf(net)
sc.parallelize(Seq(graph)).coalesce(1).saveAsTextFile(gmag + "/Graph2_" + index)
