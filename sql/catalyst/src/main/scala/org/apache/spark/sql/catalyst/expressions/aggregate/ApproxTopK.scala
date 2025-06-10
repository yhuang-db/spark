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
import org.apache.datasketches.frequencies.{ErrorType, ItemsSketch}
import org.apache.datasketches.memory.Memory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ArrayOfDecimalsSerDe, BinaryExpression, ExpectsInputTypes, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.trees.{BinaryLike, TernaryLike}
import org.apache.spark.sql.catalyst.util.{CollationFactory, GenericArrayData}
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType, TypeCollection}
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

  override def inputTypes: Seq[AbstractDataType] =
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

  override def merge(sketch: ItemsSketch[T], input: ItemsSketch[T]): ItemsSketch[T] =
    sketch.merge(input)

  override def createAggregationBuffer(): ItemsSketch[T] = new ItemsSketch[T](maxMapSize)

  override def serialize(sketch: ItemsSketch[T]): Array[Byte] = first.dataType match {
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
      sketch.toByteArray(
        new ArrayOfDecimalsSerDe(dt.precision, dt.scale).asInstanceOf[ArrayOfItemsSerDe[T]])
    // TODO: throw error for unsupported data types
  }

  override def deserialize(buffer: Array[Byte]): ItemsSketch[T] = first.dataType match {
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
      ItemsSketch.getInstance(
        Memory.wrap(buffer),
        new ArrayOfDecimalsSerDe(dt.precision, dt.scale).asInstanceOf[ArrayOfItemsSerDe[T]])
    // TODO: throw error for unsupported data types
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

  def this(child: Expression, topK: Int, maxItemsTracked: Int) =
    this(child, Literal(topK), Literal(maxItemsTracked), 0, 0)

  def this(child: Expression, topK: Expression) = this(child, topK, Literal(10000), 0, 0)

  def this(child: Expression, topK: Int) = this(child, Literal(topK), Literal(10000), 0, 0)

  def this(child: Expression) = this(child, Literal(5), Literal(10000), 0, 0)

  override def prettyName: String = "approx_top_k"

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
        case _: DecimalType => sketch.update(v.asInstanceOf[Decimal])
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
             _: FloatType | _: DoubleType | _: DateType | _: TimestampType | _: DecimalType =>
          result(i) = InternalRow.apply(row.getItem, row.getEstimate)
        case _: StringType =>
          val item = UTF8String.fromString(row.getItem.asInstanceOf[String])
          result(i) = InternalRow.apply(item, row.getEstimate)
      }
    }
    new GenericArrayData(result)
  }
}

/**
 * Abstract class for ApproxTopKAccumulate
 */
abstract class AbsApproxTopKAccumulate[T]
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

  override def dataType: DataType = BinaryType

  override def createAggregationBuffer(): ItemsSketch[T] = new ItemsSketch[T](maxMapSize)

  override def merge(buffer: ItemsSketch[T], input: ItemsSketch[T]): ItemsSketch[T] =
    buffer.merge(input)

  override def serialize(buffer: ItemsSketch[T]): Array[Byte] = {
    left.dataType match {
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
        ItemsSketch.getInstance(
          Memory.wrap(buffer),
          new ArrayOfDecimalsSerDe(dt.precision, dt.scale).asInstanceOf[ArrayOfItemsSerDe[T]])
    }
  }
}

case class ApproxTopKAccumulate(
                                 left: Expression,
                                 right: Expression,
                                 mutableAggBufferOffset: Int = 0,
                                 inputAggBufferOffset: Int = 0)
  extends AbsApproxTopKAccumulate[Any] with BinaryLike[Expression] {

  def this(child: Expression, maxItemsTracked: Expression) = this(child, maxItemsTracked, 0, 0)

  def this(child: Expression, maxItemsTracked: Int) = this(child, Literal(maxItemsTracked), 0, 0)

  def this(child: Expression) = this(child, Literal(10000), 0, 0)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ApproxTopKAccumulate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ApproxTopKAccumulate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override protected def withNewChildrenInternal(
                                                  newLeft: Expression,
                                                  newRight: Expression): ApproxTopKAccumulate =
    copy(left = newLeft, right = newRight)

  override def nullable: Boolean = false

  override def prettyName: String = "approx_top_k_accumulate"

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
        case _: DecimalType => sketch.update(v.asInstanceOf[Decimal])
      }
    }
    sketch
  }

  override def eval(buffer: ItemsSketch[Any]): Any = {
    val sketchBytes = serialize(buffer)
    val typeByte: Byte = left.dataType match {
      case _: BooleanType => 0
      case _: ByteType => 1
      case _: ShortType => 2
      case _: IntegerType => 3
      case _: LongType => 4
      case _: FloatType => 5
      case _: DoubleType => 6
      case _: DateType => 7
      case _: TimestampType => 8
      case _: StringType => 9
      case _: DecimalType => 10
    }
    val result = new Array[Byte](1 + sketchBytes.length)
    result(0) = typeByte
    System.arraycopy(sketchBytes, 0, result, 1, sketchBytes.length)
    result
  }
}

case class ApproxTopKEstimate(left: Expression, right: Expression)
  extends BinaryExpression with CodegenFallback with ExpectsInputTypes {

  def this(child: Expression, topK: Int) = this(child, Literal(topK))

  def this(child: Expression) = this(child, Literal(5))

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, IntegerType)

  private lazy val itemDataType: DataType = {
    // decide item data type based on the first byte of the binary data
    //    val inputBytes = left.eval().asInstanceOf[Array[Byte]]
    //    val typeByte = left.eval().asInstanceOf[Array[Byte]](0)
    //    typeByte match {
    //      case 0 => BooleanType
    //      case 1 => ByteType
    //      case 2 => ShortType
    //      case 3 => IntegerType
    //      case 4 => LongType
    //      case 5 => FloatType
    //      case 6 => DoubleType
    //      case 7 => DateType
    //      case 8 => TimestampType
    //      case 9 => StringType
    //      case 10 => DecimalType.SYSTEM_DEFAULT
    //    }
    // scalastyle:off
    println("debug")
    // scalastyle:on
    IntegerType
  }

  override def dataType: DataType = {
    val resultEntryType = StructType(
      StructField("Item", itemDataType, nullable = false) ::
        StructField("Estimate", LongType, nullable = false) :: Nil)
    ArrayType(resultEntryType, containsNull = false)
  }

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression)
  : Expression = copy(left = newLeft, right = newRight)

  override def nullIntolerant: Boolean = true

  override def prettyName: String = "approx_top_k_estimate"

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val inputBytes = input1.asInstanceOf[Array[Byte]]
    val sketchBytes = inputBytes.slice(1, inputBytes.length)
    val topK = input2.asInstanceOf[Int]

    val itemsSketch = ItemsSketch.getInstance(
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

    val items = itemsSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES)
    val resultLength = math.min(items.length, topK)
    val result = new Array[AnyRef](resultLength)
    for (i <- 0 until resultLength) {
      val row = items(i)
      itemDataType match {
        case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
             _: FloatType | _: DoubleType | _: DateType | _: TimestampType | _: DecimalType =>
          result(i) = InternalRow.apply(row.getItem, row.getEstimate)
        case _: StringType =>
          val item = UTF8String.fromString(row.getItem.asInstanceOf[String])
          result(i) = InternalRow.apply(item, row.getEstimate)
      }
    }
    new GenericArrayData(result)
  }
}
