/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.FastMergeOp.MergeOpType
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.{FastMergeOptimizeHeap, MergeLogic, MergeUtils}
import org.apache.spark.sql.execution.datasources.v2.merge.{FieldInfo, KeyIndex, MergePartitionedFile}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

import scala.language.postfixOps

/**
 * @param filesInfo (file, columnar reader of file)
 */
class FastMerge(filesInfo: Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])],
                resultSchema: Seq[FieldInfo],
                mergeOp: Seq[MergeOpType]) extends MergeLogic {

  //Initialize the last piece of data when the batch is switched
  private val temporaryRow: Array[Any] = new Array[Any](resultSchema.size)

  //get next batch
  val fileSeq: Seq[(MergePartitionedFile, ColumnarBatch)] = MergeUtils.getNextBatch(filesInfo)
  val mergeHeap = new FastMergeOptimizeHeap()
  mergeHeap.enqueueBySeq(MergeUtils.toBufferedIterator(fileSeq))

  def getTemporaryRow(): Array[Any] = {
    temporaryRow
  }

  private def deepCopy(): Unit = {
    temporaryRow.indices.foreach { i =>
      val v = temporaryRow(i)
      temporaryRow(i) = v match {
        case string: UTF8String =>
          string.clone()
        case _ =>
          v
      }
    }
  }

  private def shadowedCopy(row: InternalRow): Unit = {
    resultSchema.indices.foreach { i =>
      temporaryRow(i) = row.get(i, resultSchema(i).fieldType)
    }
  }

  def isHeapEmpty: Boolean = mergeHeap.isEmpty

  def merge(): Unit = {
    var lastKey: UTF8String = null
    var rowVersion: Long = -5
    while (mergeHeap.nonEmpty) {
      val currentFile = mergeHeap.dequeue()
      val currentRowAndLineId = currentFile._2.head
      val currentVersion = currentFile._1
      val row = currentRowAndLineId._1
      val currentKey = row.getUTF8String(0)

      if (lastKey == null) {
        lastKey = currentKey
        shadowedCopy(row)
        rowVersion = currentVersion
      } else {
        if (!lastKey.equals(currentKey)) {
//          mergeHeap.enqueue(currentFile)
          return
        } else {
          // do merge
          mergeOp.indices.foreach { i =>
            mergeOp(i) match {
              case FastMergeOp.Default => // no-op
              case FastMergeOp.Add =>
                temporaryRow(i) = temporaryRow(i).asInstanceOf[Long] + row.getLong(i)
              case FastMergeOp.NonNull if !row.isNullAt(i) &&
                  (temporaryRow(i) == null || MergeUtils.NullString.equals(temporaryRow(i))) =>
                temporaryRow(i) = row.getUTF8String(i).clone()
              case _ =>
            }
          }
        }
      }
      currentFile._2.next()
      if (currentFile._2.hasNext) {
        mergeHeap.enqueue(currentFile)
      } else {
        // go to next batch, need deep copy
        if (currentVersion == rowVersion) {
          deepCopy()
          rowVersion = -5
          lastKey = temporaryRow(0).asInstanceOf[UTF8String]
        }
        val fileInfo = filesInfo.filter(t => t._1.writeVersion.equals(currentVersion))
        val nextBatches = MergeUtils.getNextBatch(fileInfo)
        if (nextBatches.nonEmpty) {
          val bufferIt = MergeUtils.toBufferedIterator(nextBatches)
          mergeHeap.enqueue(bufferIt.head)
        } else {
          mergeHeap.poll()
        }
      }
    }
  }

  override def closeReadFileReader(): Unit = {
    filesInfo.foreach(f => f._2.close())
  }

  private def debugRow(ver: Long, row: InternalRow): Unit = {
    val p: String = resultSchema.indices.map { i =>
      if (row.isNullAt(i)) {
        "null"
      } else {
        row.get(i, resultSchema(i).fieldType).toString
      }
    }.reduce(_ + "," + _)
    println("row => " + ver + "," + p)
  }
}

object FastMergeOp extends Enumeration {
  type MergeOpType = Value

  val Default, NonNull, Add = Value
}
