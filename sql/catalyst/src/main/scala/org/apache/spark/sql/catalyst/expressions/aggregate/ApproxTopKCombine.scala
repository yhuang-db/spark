/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.datasketches.common.{ArrayOfBooleansSerDe, ArrayOfDoublesSerDe, ArrayOfItemsSerDe, ArrayOfLongsSerDe, ArrayOfNumbersSerDe, ArrayOfStringsSerDe, SketchesArgumentException}
import org.apache.datasketches.frequencies.ItemsSketch
import org.apache.datasketches.memory.Memory

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ArrayOfDecimalsSerDe, ExpectsInputTypes, Expression, Literal}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

class CombineInternal[T](sketch: ItemsSketch[T], itemDataType: DataType, var maxItemsTracked: Int) {
  def getSketch: ItemsSketch[T] = sketch

  def getItemDataType: DataType = itemDataType

  def getMaxItemsTracked: Int = maxItemsTracked

  def setMaxItemsTracked(maxItemsTracked: Int): Unit = {
    if (maxItemsTracked < 0) {
      throw new IllegalArgumentException("Max items tracked cannot be negative")
    }
    this.maxItemsTracked = maxItemsTracked
  }

  def itemDataTypeToByte: Byte = {
    itemDataType match {
      case _: BooleanType => 0
      case _: ByteType => 1
      case _: ShortType => 2
      case _: IntegerType => 3
      case _: FloatType => 4
      case _: DateType => 5
      case _: LongType => 6
      case _: TimestampType => 7
      case _: DoubleType => 8
      case _: StringType => 9
      case _: DecimalType => 10
    }
  }
}

abstract class AbsApproxTopKCombine[T]
  extends TypedImperativeAggregate[CombineInternal[T]]
  with ExpectsInputTypes {

  val left: Expression
  val right: Expression
  lazy val itemDataType: DataType = left.dataType.asInstanceOf[StructType]("ItemTypeNull").dataType
  lazy val combineSizeSpecified: Boolean = right.eval().asInstanceOf[Int] != -1

  def genSketchSerDe(dataType: DataType): ArrayOfItemsSerDe[T] = {
    dataType match {
      case _: BooleanType => new ArrayOfBooleansSerDe().asInstanceOf[ArrayOfItemsSerDe[T]]
      case _: ByteType | _: ShortType | _: IntegerType | _: FloatType | _: DateType =>
        new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[T]]
      case _: LongType | _: TimestampType =>
        new ArrayOfLongsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]]
      case _: DoubleType =>
        new ArrayOfDoublesSerDe().asInstanceOf[ArrayOfItemsSerDe[T]]
      case _: StringType =>
        new ArrayOfStringsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]]
      case dt: DecimalType =>
        new ArrayOfDecimalsSerDe(dt.precision, dt.scale).asInstanceOf[ArrayOfItemsSerDe[T]]
    }
  }

  def calMaxMapSize(maxItemsTracked: Int): Int = {
    val ceilMaxMapSize = math.ceil(maxItemsTracked / 0.75).toInt
    math.pow(2, math.ceil(math.log(ceilMaxMapSize) / math.log(2))).toInt
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StructType, IntegerType)

  override def dataType: DataType = StructType(
    StructField("DataSketch", BinaryType, nullable = false) ::
      StructField("ItemTypeNull", itemDataType) ::
      StructField("MaxItemsTracked", IntegerType, nullable = false) :: Nil)

  override def createAggregationBuffer(): CombineInternal[T] = {
    if (combineSizeSpecified) {
      val maxItemsTracked = right.eval().asInstanceOf[Int]
      val maxMapSize = calMaxMapSize(maxItemsTracked)
      new CombineInternal[T](new ItemsSketch[T](maxMapSize), itemDataType, maxItemsTracked)
    } else {
      new CombineInternal[T](new ItemsSketch[T](8), itemDataType, -1)
    }
  }

  override def merge(buffer: CombineInternal[T], input: CombineInternal[T]): CombineInternal[T] = {
    if (!combineSizeSpecified) {
      // check size
      if (buffer.getMaxItemsTracked == -1) {
        // If buffer is a placeholder sketch, set it to the input sketch's max items tracked
        buffer.setMaxItemsTracked(input.getMaxItemsTracked)
      }
      if (buffer.getMaxItemsTracked != input.getMaxItemsTracked) {
        throw new SparkUnsupportedOperationException(
          errorClass = "APPROX_TOP_K_SKETCH_SIZE_UNMATCHED",
          messageParameters = Map(
            "size1" -> buffer.getMaxItemsTracked.toString,
            "size2" -> input.getMaxItemsTracked.toString))
      }
    }
    buffer.getSketch.merge(input.getSketch)
    buffer
  }

  override def serialize(buffer: CombineInternal[T]): Array[Byte] = {
    val sketchBytes = buffer.getSketch.toByteArray(genSketchSerDe(buffer.getItemDataType))
    val itemTypeByte = buffer.itemDataTypeToByte
    val maxItemsTrackedByte = buffer.getMaxItemsTracked.toByte
    val byteArray = new Array[Byte](sketchBytes.length + 2)
    byteArray(0) = itemTypeByte
    byteArray(1) = maxItemsTrackedByte
    System.arraycopy(sketchBytes, 0, byteArray, 2, sketchBytes.length)
    byteArray
  }

  override def deserialize(buffer: Array[Byte]): CombineInternal[T] = {
    if (buffer.length < 2) {
      throw new IllegalArgumentException("Buffer short than 2 bytes")
    }
    val itemTypeByte = buffer(0)
    val maxItemsTracked = buffer(1).toInt
    val itemDataType = itemTypeByte match {
      case 0 => BooleanType
      case 1 => ByteType
      case 2 => ShortType
      case 3 => IntegerType
      case 4 => FloatType
      case 5 => DateType
      case 6 => LongType
      case 7 => TimestampType
      case 8 => DoubleType
      case 9 => StringType
      case 10 => DecimalType.SYSTEM_DEFAULT // Default precision and scale for DecimalType
      case _ => throw new IllegalArgumentException(s"Unknown item type byte: $itemTypeByte")
    }
    val sketchBytes = buffer.slice(2, buffer.length)
    val sketch = ItemsSketch.getInstance(Memory.wrap(sketchBytes), genSketchSerDe(itemDataType))
    new CombineInternal[T](sketch, itemDataType, maxItemsTracked)
  }
}

case class ApproxTopKCombine(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends AbsApproxTopKCombine[Any]
  with BinaryLike[Expression] {
  def this(child: Expression, maxItemsTracked: Expression) = this(child, maxItemsTracked, 0, 0)

  def this(child: Expression, maxItemsTracked: Int) = this(child, Literal(maxItemsTracked), 0, 0)

  def this(child: Expression) = this(child, Literal(-1), 0, 0)

  override def prettyName: String = "approx_top_k_combine"

  override def nullable: Boolean = false

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): ApproxTopKCombine =
    copy(left = newLeft, right = newRight)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ApproxTopKCombine =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ApproxTopKCombine =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def update(buffer: CombineInternal[Any], input: InternalRow): CombineInternal[Any] = {
    val inputSketchBytes = left.eval(input).asInstanceOf[InternalRow].getBinary(0)
    val inputMaxItemsTracked = left.eval(input).asInstanceOf[InternalRow].getInt(2)
    val inputSketch = try {
      ItemsSketch.getInstance(Memory.wrap(inputSketchBytes), genSketchSerDe(buffer.getItemDataType))
    } catch {
      case _: SketchesArgumentException =>
        throw new SparkUnsupportedOperationException(
          errorClass = "APPROX_TOP_K_SKETCH_TYPE_UNMATCHED"
        )
    }
    buffer.getSketch.merge(inputSketch)
    if (!combineSizeSpecified) {
      buffer.setMaxItemsTracked(inputMaxItemsTracked)
    }
    buffer
  }

  override def eval(buffer: CombineInternal[Any]): Any = {
    val sketchBytes = try {
      buffer.getSketch.toByteArray(genSketchSerDe(buffer.getItemDataType))
    } catch {
      case _: ArrayStoreException =>
        throw new SparkUnsupportedOperationException(
          errorClass = "APPROX_TOP_K_SKETCH_TYPE_UNMATCHED"
        )
    }
    val maxItemsTracked = buffer.getMaxItemsTracked
    InternalRow.apply(sketchBytes, null, maxItemsTracked)
  }
}
