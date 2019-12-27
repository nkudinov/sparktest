import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

class NewFormatRelation(_sqlContext: SQLContext, _schema: StructType)  extends BaseRelation with PrunedFilteredScan{
  override def sqlContext: SQLContext = _sqlContext

  override def schema: StructType = _schema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = new NewFormatRDD(sqlContext.sparkContext)

  override def toString: String =  NewFormatRelationProvider.SHORT_NAME

}
