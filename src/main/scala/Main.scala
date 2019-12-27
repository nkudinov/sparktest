import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object Main extends App{
  val spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse").config("spark.sql.codegen.wholeStage", "false").appName("Main").master("local[5]").getOrCreate

  spark.read
       .format("NewFormat")
       .schema(new StructType())
       .load()
       .explain()

  println("finish")
}
