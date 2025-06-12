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


abstract class AbsApproxTopKBase[T]
  extends TypedImperativeAggregate[ItemsSketch[T]]
  with ExpectsInputTypes {

  def calMaxMapSize(maxItemsTracked: Int): Int = {
    // The maximum capacity of this internal hash map is * 0.75 times * maxMapSize.
    val ceilMaxMapSize = math.ceil(maxItemsTracked / 0.75).toInt
    // The maxMapSize must be a power of 2 and greater than ceilMaxMapSize
    math.pow(2, math.ceil(math.log(ceilMaxMapSize) / math.log(2))).toInt
  }

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

  val allowedItemTypeCollection: TypeCollection = TypeCollection(
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
    StringTypeWithCollation(supportsTrimCollation = true))

  override def merge(buffer: ItemsSketch[T], input: ItemsSketch[T]): ItemsSketch[T] =
    buffer.merge(input)

  def updateSketchBuffer(
      sketchExpression: Expression,
      buffer: ItemsSketch[Any],
      input: InternalRow): ItemsSketch[Any] = {
    val v = sketchExpression.eval(input)
    if (v != null) {
      sketchExpression.dataType match {
        case _: BooleanType => buffer.update(v.asInstanceOf[Boolean])
        case _: ByteType => buffer.update(v.asInstanceOf[Byte])
        case _: ShortType => buffer.update(v.asInstanceOf[Short])
        case _: IntegerType => buffer.update(v.asInstanceOf[Int])
        case _: LongType => buffer.update(v.asInstanceOf[Long])
        case _: FloatType => buffer.update(v.asInstanceOf[Float])
        case _: DoubleType => buffer.update(v.asInstanceOf[Double])
        case _: DateType => buffer.update(v.asInstanceOf[Int])
        case _: TimestampType => buffer.update(v.asInstanceOf[Long])
        case st: StringType =>
          val cKey = CollationFactory.getCollationKey(v.asInstanceOf[UTF8String], st.collationId)
          buffer.update(cKey.toString)
        case _: DecimalType => buffer.update(v.asInstanceOf[Decimal])
      }
    }
    buffer
  }
}


abstract class AbsApproxTopK[T] extends AbsApproxTopKBase[T] {
  val first: Expression
  val second: Expression
  val third: Expression

  override def prettyName: String = "approx_top_k"

  override def nullable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] =
    Seq(allowedItemTypeCollection, IntegerType, IntegerType)

  override def dataType: DataType = {
    val resultStruct = StructType(
      StructField("Item", first.dataType, nullable = false) ::
        StructField("Estimate", LongType, nullable = false) :: Nil)
    ArrayType(resultStruct, containsNull = false)
  }

  override def createAggregationBuffer(): ItemsSketch[T] = {
    val maxItemsTracked = third.eval().asInstanceOf[Int]
    val maxMapSize = calMaxMapSize(maxItemsTracked)
    new ItemsSketch[T](maxMapSize)
  }

  override def serialize(buffer: ItemsSketch[T]): Array[Byte] =
    buffer.toByteArray(genSketchSerDe(first.dataType))

  override def deserialize(storageFormat: Array[Byte]): ItemsSketch[T] =
    ItemsSketch.getInstance(Memory.wrap(storageFormat), genSketchSerDe(first.dataType))
}


abstract class AbsApproxTopKAccumulate[T] extends AbsApproxTopKBase[T] {
  val left: Expression
  val right: Expression

  override def prettyName: String = "approx_top_k_accumulate"

  override def nullable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] =
    Seq(allowedItemTypeCollection, IntegerType)

  override def dataType: DataType = StructType(
    StructField("DataSketch", BinaryType, nullable = false) ::
      StructField("ItemTypeNull", left.dataType) ::
      StructField("MaxItemsTracked", IntegerType, nullable = false) :: Nil)

  override def createAggregationBuffer(): ItemsSketch[T] = {
    val maxItemsTracked = right.eval().asInstanceOf[Int]
    val maxMapSize = calMaxMapSize(maxItemsTracked)
    new ItemsSketch[T](maxMapSize)
  }

  override def serialize(buffer: ItemsSketch[T]): Array[Byte] =
    buffer.toByteArray(genSketchSerDe(left.dataType))

  override def deserialize(storageFormat: Array[Byte]): ItemsSketch[T] =
    ItemsSketch.getInstance(Memory.wrap(storageFormat), genSketchSerDe(left.dataType))
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

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ApproxTopK =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ApproxTopK =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): ApproxTopK =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def update(buffer: ItemsSketch[Any], input: InternalRow): ItemsSketch[Any] =
    updateSketchBuffer(first, buffer, input)

  override def eval(buffer: ItemsSketch[Any]): Any = {
    val topK = second.eval().asInstanceOf[Int]
    val items = buffer.getFrequentItems(ErrorType.NO_FALSE_POSITIVES)
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


case class ApproxTopKAccumulate(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends AbsApproxTopKAccumulate[Any]
  with BinaryLike[Expression] {

  def this(child: Expression, maxItemsTracked: Expression) = this(child, maxItemsTracked, 0, 0)

  def this(child: Expression, maxItemsTracked: Int) = this(child, Literal(maxItemsTracked), 0, 0)

  def this(child: Expression) = this(child, Literal(10000), 0, 0)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ApproxTopKAccumulate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ApproxTopKAccumulate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ApproxTopKAccumulate = copy(left = newLeft, right = newRight)

  override def update(buffer: ItemsSketch[Any], input: InternalRow): ItemsSketch[Any] =
    updateSketchBuffer(left, buffer, input)

  override def eval(buffer: ItemsSketch[Any]): Any = {
    val sketchBytes = serialize(buffer)
    val maxItemsTracked = right.eval().asInstanceOf[Int]
    InternalRow.apply(sketchBytes, null, maxItemsTracked)
  }

}


case class ApproxTopKEstimate(left: Expression, right: Expression)
  extends BinaryExpression
  with CodegenFallback
  with ExpectsInputTypes {

  def this(child: Expression, topK: Int) = this(child, Literal(topK))

  def this(child: Expression) = this(child, Literal(5))

  override def inputTypes: Seq[AbstractDataType] = Seq(StructType, IntegerType)

  private lazy val itemDataType: DataType = {
    // itemDataType is the type of the "ItemTypeNull" field of the output of ACCUMULATE or COMBINE
    left.dataType.asInstanceOf[StructType]("ItemTypeNull").dataType
  }

  override def dataType: DataType = {
    val resultEntryType = StructType(
      StructField("Item", itemDataType, nullable = false) ::
        StructField("Estimate", LongType, nullable = false) :: Nil)
    ArrayType(resultEntryType, containsNull = false)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)

  override def prettyName: String = "approx_top_k_estimate"

  override def nullIntolerant: Boolean = true

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val dataSketchBytes = input1.asInstanceOf[InternalRow].getBinary(0)
    val topK = input2.asInstanceOf[Int]

    val itemsSketch = ItemsSketch.getInstance(
      Memory.wrap(dataSketchBytes), itemDataType match {
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
