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

import java.util.Comparator

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.merge.KeyIndex
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.MergeHeap
import org.apache.spark.sql.types._

import scala.collection.{BufferedIterator, mutable}

trait MergeHeapCommon {
  type heapType = (Long, BufferedIterator[(InternalRow, Int)])
  val versionKeyInfoMap: Map[Long, Array[KeyIndex]]

  def enqueueBySeq(fileInfoSeq: Seq[(Long, BufferedIterator[(InternalRow, Int)])])

  def enqueue(fileInfo: (Long, BufferedIterator[(InternalRow, Int)]))

  def dequeue(): (Long, BufferedIterator[(InternalRow, Int)])

  def poll(): Unit

  def isEmpty: Boolean

  def nonEmpty: Boolean

  val comparatorT = new Comparator[heapType] {
    override def compare(x: (Long, BufferedIterator[(InternalRow, Int)]),
                         y: (Long, BufferedIterator[(InternalRow, Int)])): Int = {
      for (i <- versionKeyInfoMap(x._1).indices) {
        val inputX = x._2.head._1
        val inputY = y._2.head._1
        val ordinalX = versionKeyInfoMap(x._1)(i).index
        val ordinalY = versionKeyInfoMap(y._1)(i).index
        val comparV =
          versionKeyInfoMap(x._1)(i).keyType match {
            case StringType => inputX.getUTF8String(ordinalX).compareTo(inputY.getUTF8String(ordinalY))
            case IntegerType | DateType => inputX.getInt(ordinalX) compareTo (inputY getInt ordinalY)
            case BooleanType => inputX.getBoolean(ordinalX) compareTo inputY.getBoolean(ordinalY)
            case ByteType => inputX.getByte(ordinalX).compareTo(inputY.getByte(ordinalY))
            case ShortType => inputX.getShort(ordinalX).compareTo(inputY.getShort(ordinalY))
            case LongType | TimestampType => inputX.getLong(ordinalX).compareTo(inputY.getLong(ordinalY))
            case FloatType => inputX.getFloat(ordinalX).compareTo(inputY.getFloat(ordinalY))
            case DoubleType => inputX.getDouble(ordinalX).compareTo(inputY.getDouble(ordinalY))
            case t: DecimalType => inputX.getDecimal(ordinalX, t.precision, t.scale).compareTo(inputY.getDecimal(ordinalY, t.precision, t.scale))
            case _ => throw new RuntimeException("Unsupported data type for merge,type is " + versionKeyInfoMap(x._1)(i).keyType.getClass.getTypeName)
          }
        if (comparV != 0) {
          return comparV
        }
      }
      x._1.compareTo(y._1)
    }
  }

  val fastCmpT = new Comparator[heapType] {
    override def compare(o1: (Long, BufferedIterator[(InternalRow, Int)]),
                         o2: (Long, BufferedIterator[(InternalRow, Int)])): Int = {
      val cmp1 = o1._2.head._1.getUTF8String(0).compareTo(o2._2.head._1.getUTF8String(0))
      if (cmp1 == 0) { -o1._1.compareTo(o2._1) } else { cmp1 }
    }
  }
}

class MergeOptimizeHeap(versionKey: Map[Long, Array[KeyIndex]]) extends MergeHeapCommon {

  override val versionKeyInfoMap = versionKey

  private val mergeFileHeap = new MergeHeap[heapType](comparatorT)

  /**
    * Init merge heap, add each (write_version->bufferedIterator) into heap.
    *
    * @param fileInfoSeq (write_version, rowIterator().zipWithIndex.buffered)
    */
  def enqueueBySeq(fileInfoSeq: Seq[(Long, BufferedIterator[(InternalRow, Int)])]): Unit = {
    fileInfoSeq.foreach(itr => mergeFileHeap.add(itr))
  }

  def enqueue(fileInfo: (Long, BufferedIterator[(InternalRow, Int)])): Unit = mergeFileHeap.putBack(fileInfo)

  def dequeue(): (Long, BufferedIterator[(InternalRow, Int)]) = mergeFileHeap.peek()

  def poll(): Unit = mergeFileHeap.poll()

  def isEmpty: Boolean = mergeFileHeap.isEmpty

  def nonEmpty: Boolean = !isEmpty


}


class FastMergeOptimizeHeap() extends MergeHeapCommon {

  override val versionKeyInfoMap = Map.empty[Long, Array[KeyIndex]]

  private val mergeFileHeap = new MergeHeap[heapType](fastCmpT)

  /**
   * Init merge heap, add each (write_version->bufferedIterator) into heap.
   *
   * @param fileInfoSeq (write_version, rowIterator().zipWithIndex.buffered)
   */
  def enqueueBySeq(fileInfoSeq: Seq[(Long, BufferedIterator[(InternalRow, Int)])]): Unit = {
    fileInfoSeq.foreach(itr => mergeFileHeap.add(itr))
  }

  def enqueue(fileInfo: (Long, BufferedIterator[(InternalRow, Int)])): Unit =
    mergeFileHeap.putBack(fileInfo)

  def dequeue(): (Long, BufferedIterator[(InternalRow, Int)]) = mergeFileHeap.peek()

  def poll(): Unit = mergeFileHeap.poll()

  def isEmpty: Boolean = mergeFileHeap.isEmpty

  def nonEmpty: Boolean = !isEmpty

}

class MergePriorityQ(versionKey: Map[Long, Array[KeyIndex]]) extends MergeHeapCommon {

  override val versionKeyInfoMap = versionKey

  private val mergeFileHeap = new mutable.PriorityQueue[heapType]()(
    (x: heapType, y: heapType) => comparatorT.compare(y, x))

  override def enqueueBySeq(fileInfoSeq: Seq[(Long, BufferedIterator[(InternalRow, Int)])]): Unit = {
    mergeFileHeap.enqueue(fileInfoSeq: _*)
  }

  override def enqueue(fileInfo: (Long, BufferedIterator[(InternalRow, Int)])) = mergeFileHeap.enqueue(fileInfo)

  override def dequeue(): (Long, BufferedIterator[(InternalRow, Int)]) = mergeFileHeap.dequeue()

  override def poll(): Unit = {}

  override def isEmpty: Boolean = mergeFileHeap.isEmpty

  override def nonEmpty: Boolean = !isEmpty


}
