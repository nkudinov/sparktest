import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.sql.Row

class NewFormatRDD( sc: SparkContext) extends RDD[Row](sc, Nil){
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    Iterator(Row(1L,"Hello world"),Row(2L,"Hello 2"))
  }

  override protected def getPartitions: Array[Partition] = {
    Array(new Partition {
      override def index: Int = 0
    })
  }
}
