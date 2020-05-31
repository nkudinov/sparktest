import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SApp extends App {
  val spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
    .appName("MainApplication").master("local[*]").getOrCreate


  def RDDFromResourceFile( spark:SparkSession, fileName:String) = {
    spark.sparkContext.textFile(getClass.getResource( fileName).getFile)
  }

  case class Record5(col1:Int, col2:Int, col3:Int,col4:Int,col5:Int)

  def isInt(s:String) = {
      s forall Character.isDigit
  }
  def getIntValue(arr:Array[Int],i:Int):Int = {
    if (i >= arr.size) 0 else arr(i)
  }

  def parseAndFilter(rdd:RDD[String]):RDD[Record5] ={
    rdd.map(_.split(" ").filter(isInt).map(_.toInt))
       .map(e => Record5(getIntValue(e,0),getIntValue(e,1),getIntValue(e,2),getIntValue(e,3),getIntValue(e,4)))

  }

  val rdd1 = parseAndFilter( RDDFromResourceFile(spark, "file1.txt"))
  val rdd2 = parseAndFilter( RDDFromResourceFile(spark, "file2.txt"))

  val both = rdd1.union(rdd2)

  import spark.implicits._
  val ds = spark.createDataset(both)
  ds.show()

  ds.groupBy().avg("col1","col2","col3","col4","col5").show()

}
