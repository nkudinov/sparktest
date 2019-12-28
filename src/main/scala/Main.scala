
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Main extends App{
  val spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse").config("spark.sql.codegen.wholeStage", "false").appName("MainApplication Demo").master("local[5]").getOrCreate

  val schema = new StructType(Array( StructField("id",LongType) , StructField("name", StringType)))

  spark.read
       .format("NewFormat")
       .schema(schema)
       .load()
       .show()

  import spark.implicits._

  val fdf = spark.sqlContext.createDataset(new FibonacciRDD(spark.sparkContext,10))
  fdf.show()
}
