package tungsten

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder
object Main extends App {

  val rowWriter = new UnsafeRowWriter(2)
  rowWriter.write(0,11L)
  rowWriter.write(1,"world".getBytes)
  println(rowWriter.getRow.getBytes.mkString(","))

}
