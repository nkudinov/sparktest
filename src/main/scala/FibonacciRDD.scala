import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

class FibonacciRDD(sc: SparkContext, n: Int) extends RDD[Long](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    Array(new Partition {
      override def index: Int = 0
    })
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Long] = {
    new Iterator[Long]() {

      var ind = 0
      var i   = 0
      var j   = 1
      override def hasNext: Boolean = ind < n

      override def next(): Long = {
        ind += 1
        val t = j
        j = i + j
        i = t
        t
      }
    }
  }
}
