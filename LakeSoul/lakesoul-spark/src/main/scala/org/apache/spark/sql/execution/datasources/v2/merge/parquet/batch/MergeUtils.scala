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

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.{FieldIndex, MergeColumnIndex, MergeColumnarBatchNew, MergeOperator}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{BufferedIterator, mutable}

object MergeUtils {

  val NullString = UTF8String.fromString("null")

  /**
    * to Buffer Iterator
    *
    * @param seq
    * @return
    */
  def toBufferedIterator(seq: Seq[(MergePartitionedFile, ColumnarBatch)]): Seq[(Long, BufferedIterator[(InternalRow, Int)])] = {
    seq.map(tuple => tuple._1.writeVersion -> tuple._2.rowIterator().asScala.zipWithIndex.buffered)
  }

  /**
    * @param filesInfo
    * @return
    */
  def getNextBatch(filesInfo: Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])]):
  Seq[(MergePartitionedFile, ColumnarBatch)] = {
    filesInfo.filter(fileInfoReader => fileInfoReader._2.next())
      .map(fileInfoReader => fileInfoReader._1 -> fileInfoReader._2.get())
  }


  //initialize mergeBatchColumnIndex
  def initMergeBatchAndMergeIndex(fileSeq: Seq[(MergePartitionedFile, ColumnarBatch)],
                                  mergeColumnIndexMap: mutable.Map[Long, Array[Int]]): Unit = {
    val versionWithColumnNumArr: Array[(Long, Int)] = fileSeq.sortWith((t1, t2) => t1._1.writeVersion < t2._1.writeVersion)
      .toArray.map(t => (t._1.writeVersion, t._2.numCols()))

    var lastLen = 0
    for (i <- versionWithColumnNumArr.indices) {
      var end: Int = 0
      for (j <- 0 to i) {
        end += versionWithColumnNumArr(j)._2
      }
      if (i != 0) {
        lastLen += versionWithColumnNumArr(i - 1)._2
      }
      mergeColumnIndexMap += versionWithColumnNumArr(i)._1 -> Range(lastLen, end).toArray
    }

  }

  //initialize mergeColumnarBatch object
  def initMergeBatchNew(fileSeq: Seq[(MergePartitionedFile, ColumnarBatch)],
                        mergeOps: Seq[MergeOperator[Any]],
                        indexTypeArray: Seq[FieldIndex]): MergeColumnarBatchNew = {
    val arrayColumn: Array[ColumnVector] =
      fileSeq.sortWith((t1, t2) => t1._1.writeVersion < t2._1.writeVersion).toArray
        .map(t => {
          Range(0, t._2.numCols()).map(t._2.column)
        })
        .flatMap(_.toSeq)
    new MergeColumnarBatchNew(arrayColumn, mergeOps, indexTypeArray)
  }


  def resetBatchIndex(resultIndex: Array[(Integer, Integer)]): Unit = {
    for (i <- resultIndex.indices) {
      resultIndex(i) = (-1, -1)
    }
  }

  def intBatchIndexMerge(resultIndex: Array[ArrayBuffer[MergeColumnIndex]]): Unit = {
    for (i <- resultIndex.indices) {
      resultIndex(i) = new ArrayBuffer[MergeColumnIndex]()
    }
  }

  def resetBatchIndexMerge(resultIndex: Array[ArrayBuffer[MergeColumnIndex]]): Unit = {
    for (i <- resultIndex.indices) {
      resultIndex(i).clear()
    }
  }

  def initTemporaryRow(temporaryRow: Array[ArrayBuffer[Any]]): Unit = {
    for (i <- temporaryRow.indices) {
      temporaryRow(i) = new ArrayBuffer[Any]()
    }
  }

  def resetTemporaryRow(temporaryRow: Array[ArrayBuffer[Any]]): Unit = {
    for (i <- temporaryRow.indices) {
      temporaryRow(i).clear()
    }
  }


}
