package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.{FieldInfo, MergePartitionedFile}
import org.apache.spark.sql.vectorized.ColumnarBatch


class FastMergeReader[T](filesInfo: Seq[Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])]],
                         mergeOperatorInfo: Map[String, MergeOperator[Any]])
    extends PartitionReader[InternalRow] with Logging {

  private val filesItr: Iterator[Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])]] = filesInfo.iterator
  private var mergeLogic: FastMerge = _
  private val resultSchema: Seq[FieldInfo] = filesInfo.head.head._1.resultSchema
  private val mergeOp = resultSchema.map { s =>
    val fieldName = s.fieldName
    mergeOperatorInfo.get(fieldName) match {
      case Some(op) =>
        if (op.isInstanceOf[MergeOpLong]) {
          FastMergeOp.Add
        } else if (op.isInstanceOf[MergeNonNullOp[String]]) {
          FastMergeOp.NonNull
        } else {
          throw new UnsupportedOperationException()
        }
      case _ => FastMergeOp.Default
    }
  }

  /**
   * @return Boolean
   */
  override def next(): Boolean = {
    if (mergeLogic == null) {
      if (filesItr.hasNext) {
        val nextFiles = filesItr.next()
        if (nextFiles.isEmpty) {
          return false
        } else {
          mergeLogic = new FastMerge(nextFiles, resultSchema, mergeOp)
        }
      } else {
        return false
      }
    }

    if (mergeLogic.isHeapEmpty) {
      if (filesItr.hasNext) {
        //close current file readers
        mergeLogic.closeReadFileReader()
        mergeLogic = new FastMerge(filesItr.next(), resultSchema, mergeOp)
      } else {
        return false
      }
    }

    mergeLogic.merge()
    true
  }

  /**
   * @return InternalRow
   */
  override def get(): InternalRow = {
    val temporaryRow = mergeLogic.getTemporaryRow()
    val arrayRow = new GenericInternalRow(temporaryRow)
    arrayRow
  }

  override def close(): Unit = {
    if (filesInfo.nonEmpty) {
      filesInfo.foreach(f => f.foreach(_._2.close()))
    }
  }
}
