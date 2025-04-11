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

import org.apache.datasketches.common._
import org.apache.datasketches.frequencies.{ErrorType, ItemsSketch}
import org.apache.datasketches.memory.Memory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ArrayOfDecimalByteArrSerDe, ExpectsInputTypes, Expression, Literal}
import org.apache.spark.sql.catalyst.trees.{BinaryLike, TernaryLike}
import org.apache.spark.sql.catalyst.util.{CollationFactory, GenericArrayData}
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


/**
 * Abstract class for ApproxTopK
 */
abstract class AbsApproxTopK[T]
  extends TypedImperativeAggregate[ItemsSketch[T]]
    with ExpectsInputTypes {

  val first: Expression
  val second: Expression
  val third: Expression

  lazy val maxMapSize: Int = {
    val maxItemsTracked = third.eval().asInstanceOf[Int]
    // The maximum capacity of this internal hash map is * 0.75 times * maxMapSize.
    val ceilMaxMapSize = math.ceil(maxItemsTracked / 0.75).toInt
    // The maxMapSize must be a power of 2 and greater than ceilMaxMapSize
    val maxMapSize = math.pow(2, math.ceil(math.log(ceilMaxMapSize) / math.log(2))).toInt
    maxMapSize
  }

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(
      TypeCollection(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        DecimalType,
        FloatType,
        DoubleType,
        DateType,
        TimestampType,
        StringTypeWithCollation(supportsTrimCollation = true)),
      IntegerType,
      IntegerType)
  }

  override def createAggregationBuffer(): ItemsSketch[T] = new ItemsSketch[T](maxMapSize)

  override def merge(sketch: ItemsSketch[T], input: ItemsSketch[T]): ItemsSketch[T] =
    sketch.merge(input)

  override def serialize(sketch: ItemsSketch[T]): Array[Byte] = {
    first.dataType match {
      case _: BooleanType =>
        sketch.toByteArray(new ArrayOfBooleansSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: ByteType | _: ShortType | _: IntegerType | _: FloatType | _: DateType =>
        sketch.toByteArray(new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: LongType | _: TimestampType =>
        sketch.toByteArray(new ArrayOfLongsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: DoubleType =>
        sketch.toByteArray(new ArrayOfDoublesSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: StringType =>
        sketch.toByteArray(new ArrayOfStringsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case dt: DecimalType =>
        val precision = dt.precision
        if (precision <= Decimal.MAX_INT_DIGITS) {
          sketch.toByteArray(new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
        } else if (precision <= Decimal.MAX_LONG_DIGITS) {
          sketch.toByteArray(new ArrayOfLongsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
        } else {
          sketch.toByteArray(
            new ArrayOfDecimalByteArrSerDe(dt.precision, dt.scale)
              .asInstanceOf[ArrayOfItemsSerDe[T]])
        }
    }
  }

  override def deserialize(buffer: Array[Byte]): ItemsSketch[T] = {
    first.dataType match {
      case _: BooleanType =>
        ItemsSketch.getInstance(
          Memory.wrap(buffer), new ArrayOfBooleansSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: ByteType | _: ShortType | _: IntegerType | _: FloatType | _: DateType =>
        ItemsSketch.getInstance(
          Memory.wrap(buffer), new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: LongType | _: TimestampType =>
        ItemsSketch.getInstance(
          Memory.wrap(buffer), new ArrayOfLongsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: DoubleType =>
        ItemsSketch.getInstance(
          Memory.wrap(buffer), new ArrayOfDoublesSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: StringType =>
        ItemsSketch.getInstance(
          Memory.wrap(buffer), new ArrayOfStringsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case dt: DecimalType =>
        val precision = dt.precision
        if (precision <= Decimal.MAX_INT_DIGITS) {
          ItemsSketch.getInstance(
            Memory.wrap(buffer),
            new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
        } else if (precision <= Decimal.MAX_LONG_DIGITS) {
          ItemsSketch.getInstance(
            Memory.wrap(buffer),
            new ArrayOfLongsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
        } else {
          ItemsSketch.getInstance(
            Memory.wrap(buffer),
            new ArrayOfDecimalByteArrSerDe(dt.precision, dt.scale)
              .asInstanceOf[ArrayOfItemsSerDe[T]])
        }
    }
  }
}

case class ApproxTopK(
                       first: Expression,
                       second: Expression,
                       third: Expression,
                       mutableAggBufferOffset: Int = 0,
                       inputAggBufferOffset: Int = 0)
  extends AbsApproxTopK[Any]
    with TernaryLike[Expression] {

  def this(child: Expression, topK: Expression, maxItemsTracked: Expression) =
    this(child, topK, maxItemsTracked, 0, 0)


  def this(child: Expression, topK: Expression) = this(child, topK, Literal(10000), 0, 0)

  def this(child: Expression) = this(child, Literal(5), Literal(10000), 0, 0)

  def this(child: Expression, topK: Int, maxItemsTracked: Int) =
    this(child, Literal(topK), Literal(maxItemsTracked), 0, 0)

  def this(child: Expression, topK: Int) = this(child, Literal(topK), Literal(10000), 0, 0)

  override def prettyName: String = "ter_approx_top_k"

  override def nullable: Boolean = false

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ApproxTopK =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ApproxTopK =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
                                                  newFirst: Expression,
                                                  newSecond: Expression,
                                                  newThird: Expression): ApproxTopK =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def dataType: DataType = {
    val resultEntryType = StructType(
      StructField("Item", first.dataType, nullable = false) ::
        StructField("Estimate", LongType, nullable = false) :: Nil)
    ArrayType(resultEntryType, containsNull = false)
  }

  override def update(sketch: ItemsSketch[Any], input: InternalRow): ItemsSketch[Any] = {
    val v = first.eval(input)
    if (v != null) {
      first.dataType match {
        case _: BooleanType => sketch.update(v.asInstanceOf[Boolean])
        case _: ByteType => sketch.update(v.asInstanceOf[Byte])
        case _: ShortType => sketch.update(v.asInstanceOf[Short])
        case _: IntegerType => sketch.update(v.asInstanceOf[Int])
        case _: LongType => sketch.update(v.asInstanceOf[Long])
        case _: FloatType => sketch.update(v.asInstanceOf[Float])
        case _: DoubleType => sketch.update(v.asInstanceOf[Double])
        case _: DateType => sketch.update(v.asInstanceOf[Int])
        case _: TimestampType => sketch.update(v.asInstanceOf[Long])
        case st: StringType =>
          val cKey = CollationFactory.getCollationKey(v.asInstanceOf[UTF8String], st.collationId)
          sketch.update(cKey.toString)
        case dt: DecimalType =>
          if (dt.precision <= Decimal.MAX_INT_DIGITS) {
            sketch.update(v.asInstanceOf[Decimal].toUnscaledLong.toInt)
          } else if (dt.precision <= Decimal.MAX_LONG_DIGITS) {
            sketch.update(v.asInstanceOf[Decimal].toUnscaledLong)
          } else {
            sketch.update(v.asInstanceOf[Decimal])
          }
      }
    }
    sketch
  }

  override def eval(sketch: ItemsSketch[Any]): Any = {
    val topK = second.eval().asInstanceOf[Int]
    val items = sketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES)
    val resultLength = math.min(items.length, topK)
    val result = new Array[AnyRef](resultLength)
    for (i <- 0 until resultLength) {
      val row = items(i)
      first.dataType match {
        case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
             _: FloatType | _: DoubleType | _: DateType | _: TimestampType =>
          result(i) = InternalRow.apply(row.getItem, row.getEstimate)
        case _: StringType =>
          val item = UTF8String.fromString(row.getItem.asInstanceOf[String])
          result(i) = InternalRow.apply(item, row.getEstimate)
        case dt: DecimalType =>
          if (dt.precision <= Decimal.MAX_INT_DIGITS) {
            val intItem = row.getItem.asInstanceOf[Int]
            val decimalItem = Decimal.createUnsafe(intItem, dt.precision, dt.scale)
            result(i) = InternalRow.apply(decimalItem, row.getEstimate)
          } else if (dt.precision <= Decimal.MAX_LONG_DIGITS) {
            val longItem = row.getItem.asInstanceOf[Long]
            val decimalItem = Decimal.createUnsafe(longItem, dt.precision, dt.scale)
            result(i) = InternalRow.apply(decimalItem, row.getEstimate)
          } else {
            result(i) = InternalRow.apply(row.getItem, row.getEstimate)
          }
      }
    }
    new GenericArrayData(result)
  }
}


/**
 * Abstract class for ApproxTopKAccumulate
 */
abstract class AbsApproxTopKAcc[T]
  extends TypedImperativeAggregate[ItemsSketch[T]]
    with ExpectsInputTypes {

  val left: Expression
  val right: Expression

  lazy val maxMapSize: Int = {
    val maxItemsTracked = right.eval().asInstanceOf[Int]
    // The maximum capacity of this internal hash map is * 0.75 times * maxMapSize.
    val ceilMaxMapSize = math.ceil(maxItemsTracked / 0.75).toInt
    // The maxMapSize must be a power of 2 and greater than ceilMaxMapSize
    val maxMapSize = math.pow(2, math.ceil(math.log(ceilMaxMapSize) / math.log(2))).toInt
    maxMapSize
  }

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(
      TypeCollection(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        DecimalType,
        FloatType,
        DoubleType,
        DateType,
        TimestampType,
        StringTypeWithCollation(supportsTrimCollation = true)),
      IntegerType)
  }

  override def createAggregationBuffer(): ItemsSketch[T] = new ItemsSketch[T](maxMapSize)

  override def merge(sketch: ItemsSketch[T], input: ItemsSketch[T]): ItemsSketch[T] =
    sketch.merge(input)

  override def serialize(sketch: ItemsSketch[T]): Array[Byte] = {
    left.dataType match {
      case _: BooleanType =>
        sketch.toByteArray(new ArrayOfBooleansSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: ByteType | _: ShortType | _: IntegerType | _: FloatType | _: DateType =>
        sketch.toByteArray(new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: LongType | _: TimestampType =>
        sketch.toByteArray(new ArrayOfLongsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: DoubleType =>
        sketch.toByteArray(new ArrayOfDoublesSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: StringType =>
        sketch.toByteArray(new ArrayOfStringsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case dt: DecimalType =>
        val precision = dt.precision
        if (precision <= Decimal.MAX_INT_DIGITS) {
          sketch.toByteArray(new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
        } else if (precision <= Decimal.MAX_LONG_DIGITS) {
          sketch.toByteArray(new ArrayOfLongsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
        } else {
          sketch.toByteArray(
            new ArrayOfDecimalByteArrSerDe(dt.precision, dt.scale)
              .asInstanceOf[ArrayOfItemsSerDe[T]])
        }
    }
  }

  override def deserialize(buffer: Array[Byte]): ItemsSketch[T] = {
    left.dataType match {
      case _: BooleanType =>
        ItemsSketch.getInstance(
          Memory.wrap(buffer), new ArrayOfBooleansSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: ByteType | _: ShortType | _: IntegerType | _: FloatType | _: DateType =>
        ItemsSketch.getInstance(
          Memory.wrap(buffer), new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: LongType | _: TimestampType =>
        ItemsSketch.getInstance(
          Memory.wrap(buffer), new ArrayOfLongsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: DoubleType =>
        ItemsSketch.getInstance(
          Memory.wrap(buffer), new ArrayOfDoublesSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case _: StringType =>
        ItemsSketch.getInstance(
          Memory.wrap(buffer), new ArrayOfStringsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
      case dt: DecimalType =>
        val precision = dt.precision
        if (precision <= Decimal.MAX_INT_DIGITS) {
          ItemsSketch.getInstance(
            Memory.wrap(buffer),
            new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
        } else if (precision <= Decimal.MAX_LONG_DIGITS) {
          ItemsSketch.getInstance(
            Memory.wrap(buffer),
            new ArrayOfLongsSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
        } else {
          ItemsSketch.getInstance(
            Memory.wrap(buffer),
            new ArrayOfDecimalByteArrSerDe(dt.precision, dt.scale)
              .asInstanceOf[ArrayOfItemsSerDe[T]])
        }
    }
  }
}

case class ApproxTopKAccumulate(
                                 left: Expression,
                                 right: Expression,
                                 mutableAggBufferOffset: Int = 0,
                                 inputAggBufferOffset: Int = 0)
  extends AbsApproxTopKAcc[Any]
    with BinaryLike[Expression] {

  def this(child: Expression, maxItemsTracked: Expression) = this(child, maxItemsTracked, 0, 0)

  def this(child: Expression, maxItemsTracked: Int) = this(child, Literal(maxItemsTracked), 0, 0)

  def this(child: Expression) = this(child, Literal(10000), 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ApproxTopKAccumulate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ApproxTopKAccumulate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression):
  ApproxTopKAccumulate = copy(left = newLeft, right = newRight)

  override def nullable: Boolean = false

  override def prettyName: String = "approx_top_k_accumulate"

  override def dataType: DataType = BinaryType

  override def eval(sketch: ItemsSketch[Any]): Any = this.serialize(sketch)

  override def update(sketch: ItemsSketch[Any], input: InternalRow): ItemsSketch[Any] = {
    val v = left.eval(input)
    if (v != null) {
      left.dataType match {
        case _: BooleanType => sketch.update(v.asInstanceOf[Boolean])
        case _: ByteType => sketch.update(v.asInstanceOf[Byte])
        case _: ShortType => sketch.update(v.asInstanceOf[Short])
        case _: IntegerType => sketch.update(v.asInstanceOf[Int])
        case _: LongType => sketch.update(v.asInstanceOf[Long])
        case _: FloatType => sketch.update(v.asInstanceOf[Float])
        case _: DoubleType => sketch.update(v.asInstanceOf[Double])
        case _: DateType => sketch.update(v.asInstanceOf[Int])
        case _: TimestampType => sketch.update(v.asInstanceOf[Long])
        case st: StringType =>
          val cKey = CollationFactory.getCollationKey(v.asInstanceOf[UTF8String], st.collationId)
          sketch.update(cKey.toString)
        case dt: DecimalType =>
          if (dt.precision <= Decimal.MAX_INT_DIGITS) {
            sketch.update(v.asInstanceOf[Decimal].toUnscaledLong.toInt)
          } else if (dt.precision <= Decimal.MAX_LONG_DIGITS) {
            sketch.update(v.asInstanceOf[Decimal].toUnscaledLong)
          } else {
            sketch.update(v.asInstanceOf[Decimal])
          }
      }
    }
    sketch
  }
}
