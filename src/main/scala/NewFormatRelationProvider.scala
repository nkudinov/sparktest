import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, Filter, PrunedFilteredScan, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class NewFormatRelationProvider extends DataSourceRegister with SchemaRelationProvider{
  override def shortName(): String = "NewFormat"

  override def createRelation(_sqlContext: SQLContext, parameters: Map[String, String], _schema: StructType): BaseRelation = {
    new BaseRelation with PrunedFilteredScan {
      override def sqlContext: SQLContext = _sqlContext

      override def schema: StructType = _schema

      override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
        new RDD[Row](sqlContext.sparkContext, Nil) {
          override def compute(split: Partition, context: TaskContext): Iterator[Row] = ???

          override protected def getPartitions: Array[Partition] = ???
        }
      }
    }
  }
}
