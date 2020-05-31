import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SApp extends App {
  val spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
    .appName("MainApplication").master("local[*]").getOrCreate


  def RDDFromResourceFile( spark:SparkSession, fileName:String) = {
    spark.sparkContext.textFile(getClass.getResource( fileName).getFile)
  }

  case class IntRecord(col1:Int, col2:Int, col3:Int,col4:Int,col5:Int)

  def isInt(s:String) = {
      s forall Character.isDigit
  }

  def parseAndFilter(rdd:RDD[String]):RDD[IntRecord] ={
    rdd.map{
      line => val arr = line.split(" "); (arr(0),arr(1),arr(2),arr(3),arr(4))
    }.filter {
      e => isInt(e._1) && isInt(e._2) && isInt(e._3) && isInt(e._4) && isInt(e._5)
    }.map{
      e => IntRecord( e._1.toInt,e._2.toInt,e._3.toInt,e._4.toInt,e._5.toInt)
    }
  }

  val rdd1 = parseAndFilter( RDDFromResourceFile(spark, "file1.txt"))
  val rdd2 = parseAndFilter( RDDFromResourceFile(spark, "file2.txt"))

  val both = rdd1.union(rdd2)

  import spark.implicits._
  val ds = spark.createDataset(both)

  ds.groupBy().avg("col1","col2","col3","col4","col5").show()

}
