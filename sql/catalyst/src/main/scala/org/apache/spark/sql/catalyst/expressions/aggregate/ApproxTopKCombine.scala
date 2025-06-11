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

import org.apache.datasketches.common.{ArrayOfBooleansSerDe, ArrayOfDoublesSerDe, ArrayOfItemsSerDe, ArrayOfLongsSerDe, ArrayOfNumbersSerDe, ArrayOfStringsSerDe}
import org.apache.datasketches.frequencies.ItemsSketch
import org.apache.datasketches.memory.Memory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ArrayOfDecimalsSerDe, ExpectsInputTypes, Expression, Literal}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}


abstract class AbsApproxTopKCombine[T]
  extends TypedImperativeAggregate[ItemsSketch[T]]
  with ExpectsInputTypes {

  val left: Expression
  val right: Expression

  override def inputTypes: Seq[AbstractDataType] = Seq(StructType, IntegerType)

  lazy val itemDataType: DataType = left.dataType.asInstanceOf[StructType]("ItemTypeNull").dataType

  override def dataType: DataType = StructType(
    StructField("DataSketch", BinaryType, nullable = false) ::
      StructField("ItemTypeNull", itemDataType) ::
      StructField("MaxItemsTracked", IntegerType, nullable = false) :: Nil)

  var firstSketchCount: Option[Int] = None

  lazy val combineSizeSpecified: Boolean = right.eval().asInstanceOf[Int] != -1

  override def createAggregationBuffer(): ItemsSketch[T] = {
    if (combineSizeSpecified) {
      val maxItemsTracked = right.eval().asInstanceOf[Int]
      val ceilMaxMapSize = math.ceil(maxItemsTracked / 0.75).toInt
      val maxMapSize = math.pow(2, math.ceil(math.log(ceilMaxMapSize) / math.log(2))).toInt
      // scalastyle:off
      println(s"createAggregationBuffer: combine size specified, create buffer with size $maxMapSize")
      // scalastyle:on
      new ItemsSketch[T](maxMapSize)
    } else {
      // scalastyle:off
      println("createAggregationBuffer: combine size NOT specified, create a placeholder sketch")
      // scalastyle:on
      new ItemsSketch[T](8)
    }
  }

  override def merge(buffer: ItemsSketch[T], input: ItemsSketch[T]): ItemsSketch[T] =
    buffer.merge(input)

  override def serialize(buffer: ItemsSketch[T]): Array[Byte] = {
    itemDataType match {
      case _: BooleanType =>
        buffer.toByteArray(new ArrayOfBooleansSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: ByteType | _: ShortType | _: IntegerType | _: FloatType | _: DateType =>
        buffer.toByteArray(new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: LongType | _: TimestampType =>
        buffer.toByteArray(new ArrayOfLongsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: DoubleType =>
        buffer.toByteArray(new ArrayOfDoublesSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: StringType =>
        buffer.toByteArray(new ArrayOfStringsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case dt: DecimalType =>
        buffer.toByteArray(
          new ArrayOfDecimalsSerDe(dt.precision, dt.scale).asInstanceOf[ArrayOfItemsSerDe[T]])
    }
  }

  override def deserialize(buffer: Array[Byte]): ItemsSketch[T] = {
    ItemsSketch.getInstance(
      Memory.wrap(buffer), itemDataType match {
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
    )
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

  override def nullable: Boolean = false

  override def prettyName: String = "approx_top_k_combine"

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ApproxTopKCombine = copy(left = newLeft, right = newRight)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ApproxTopKCombine =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ApproxTopKCombine =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def update(buffer: ItemsSketch[Any], input: InternalRow): ItemsSketch[Any] = {
    val sketchBytes = left.eval(input).asInstanceOf[InternalRow].getBinary(0)
    val currMaxItemsTracked = left.eval(input).asInstanceOf[InternalRow].getInt(2)

    val checkedSizeBuffer = if (combineSizeSpecified) {
      // scalastyle:off
      println("update: combine size specified, use existing buffer")
      // scalastyle:on
      buffer
    } else {
      firstSketchCount match {
        case Some(size) if size == currMaxItemsTracked => buffer
        case Some(size) if size != currMaxItemsTracked =>
          throw new IllegalArgumentException(
            s"All sketches must have the same max items tracked, " +
              s"but found $currMaxItemsTracked and $size")
        case None =>
          firstSketchCount = Some(currMaxItemsTracked)
          val ceilMaxMapSize = math.ceil(currMaxItemsTracked / 0.75).toInt
          val maxMapSize = math.pow(2, math.ceil(math.log(ceilMaxMapSize) / math.log(2))).toInt
          // scalastyle:off
          println(s"update: re-set buffer size to $maxMapSize")
          // scalastyle:on
          new ItemsSketch[Any](maxMapSize)
        case _ =>
          throw new IllegalArgumentException(
            s"Wired firstSketchCount: $firstSketchCount, " +
              s"currMaxItemsTracked: $currMaxItemsTracked")
      }
    }

    if (sketchBytes != null) {
      val inputSketch = ItemsSketch.getInstance(
        Memory.wrap(sketchBytes), itemDataType match {
          case _: BooleanType => new ArrayOfBooleansSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
          case _: ByteType | _: ShortType | _: IntegerType | _: FloatType | _: DateType =>
            new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
          case _: LongType | _: TimestampType =>
            new ArrayOfLongsSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
          case _: DoubleType =>
            new ArrayOfDoublesSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
          case _: StringType =>
            new ArrayOfStringsSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
          case dt: DecimalType =>
            new ArrayOfDecimalsSerDe(dt.precision, dt.scale).asInstanceOf[ArrayOfItemsSerDe[Any]]
        })
      checkedSizeBuffer.merge(inputSketch)
    }
    checkedSizeBuffer
  }

  override def eval(buffer: ItemsSketch[Any]): Any = {
    val sketchBytes = serialize(buffer)
    val maxItemsTracked = right.eval().asInstanceOf[Int]
    InternalRow.apply(sketchBytes, null, maxItemsTracked)
  }
}
