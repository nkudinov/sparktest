import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, Filter, PrunedFilteredScan, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class NewFormatRelationProvider extends DataSourceRegister with SchemaRelationProvider {
  override def shortName(): String = NewFormatRelationProvider.SHORT_NAME

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = new NewFormatRelation( sqlContext, schema)

}
object NewFormatRelationProvider{
  val SHORT_NAME = "NewFormat"
}
