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

/**
 * ApproxTopK is an aggregate function that returns the top K frequent items in a dataset.
 * It uses the ItemsSketch from the DataSketches library to maintain an approximate count of items.
 * It returns an array of structs, where each struct contains an item and its estimated count.
 *
 * @param first  Column name or expression whose top K items are to be estimated.
 * @param second K: the number of top items to return.
 * @param third  The maximum number of frequent items to track.
 */
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

  override def dataType: DataType = StructType(
    StructField("DataSketch", BinaryType, nullable = false) ::
      StructField("ItemTypeNull", left.dataType) ::
      StructField("MaxItemsTracked", IntegerType) :: Nil)

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

/**
 * ApproxTopKAccumulate is an aggregate function that accumulates the top K frequent items,
 * and return the DataSketch in binary format.
 *
 * @param left  Column name or expression whose top K items are to be estimated.
 * @param right The maximum number of frequent items to track in the sketch.
 */
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
    val maxItemsTracked = right.eval().asInstanceOf[Int]
    InternalRow.apply(sketchBytes, null, maxItemsTracked)
  }
}

/**
 * ApproxTopKEstimate is an expression that estimates the top K frequent items from a DataSketch.
 * It takes a DataSketch in binary format and returns an array of structs, where each struct
 * contains an item and its estimated count.
 *
 * @param left  The DataSketch in binary format.
 * @param right K: the number of top items to return.
 */
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

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression)
  : Expression = copy(left = newLeft, right = newRight)

  override def nullIntolerant: Boolean = true

  override def prettyName: String = "approx_top_k_estimate"

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

abstract class AbsApproxTopKCombine[T]
  extends TypedImperativeAggregate[ItemsSketch[T]]
    with ExpectsInputTypes {

  val left: Expression
  val right: Expression

  override def inputTypes: Seq[AbstractDataType] = Seq(StructType, IntegerType)

  lazy val itemDataType: DataType = {
    // itemDataType is the type of the "ItemTypeNull" field of the output of ACCUMULATE or COMBINE
    left.dataType.asInstanceOf[StructType]("ItemTypeNull").dataType
  }

  override def dataType: DataType = StructType(
    StructField("DataSketch", BinaryType, nullable = false) ::
      StructField("ItemTypeNull", itemDataType) ::
      StructField("MaxItemsTracked", IntegerType, nullable = false) :: Nil)

  var firstSketchCount: Option[Int] = None

  lazy val combineSizeSpecified: Boolean = right.eval().asInstanceOf[Int] != -1

  override def createAggregationBuffer(): ItemsSketch[T] = {
    combineSizeSpecified match {
      case true =>
        val maxItemsTracked = right.eval().asInstanceOf[Int]
        // The maximum capacity of this internal hash map is * 0.75 times * maxMapSize.
        val ceilMaxMapSize = math.ceil(maxItemsTracked / 0.75).toInt
        // The maxMapSize must be a power of 2 and greater than ceilMaxMapSize
        val maxMapSize = math.pow(2, math.ceil(math.log(ceilMaxMapSize) / math.log(2))).toInt
        // scalastyle:off
        println(s"createAggregationBuffer: Combine size specified, create buffer with size $maxMapSize")
        // scalastyle:on
        new ItemsSketch[T](maxMapSize)
      case false =>
        // scalastyle:off
        println("createAggregationBuffer: Combine size not specified, create a placeholder sketch with size 8")
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
    itemDataType match {
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

case class ApproxTopKCombine(
                              left: Expression,
                              right: Expression,
                              mutableAggBufferOffset: Int = 0,
                              inputAggBufferOffset: Int = 0)
  extends AbsApproxTopKCombine[Any]
    with BinaryLike[Expression] {

  def this(child: Expression, maxItemsTracked: Expression) =
    this(child, maxItemsTracked, 0, 0)

  def this(child: Expression, maxItemsTracked: Int) =
    this(child, Literal(maxItemsTracked), 0, 0)

  def this(child: Expression) = this(child, Literal(-1), 0, 0)

  //  def this(child: Expression) = this(child, Literal(10000), 0, 0)

  override def nullable: Boolean = false

  override def prettyName: String = "approx_top_k_combine"

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression)
  : ApproxTopKCombine = copy(left = newLeft, right = newRight)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ApproxTopKCombine =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ApproxTopKCombine =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def update(buffer: ItemsSketch[Any], input: InternalRow): ItemsSketch[Any] = {
    val sketchBytes = left.eval(input).asInstanceOf[InternalRow].getBinary(0)
    val currMaxItemsTracked = left.eval(input).asInstanceOf[InternalRow].getInt(2)

    val checkedSizeBuffer = if (combineSizeSpecified) {
      // scalastyle:off
      println("update: Combine size specified, re-use the buffer")
      // scalastyle:on
      buffer
    } else {
      firstSketchCount match {
        case Some(size) if size == currMaxItemsTracked => buffer
        case Some(size) if size != currMaxItemsTracked =>
          throw new IllegalArgumentException(
            s"All sketches must have the same max items tracked, " +
              s"but found ${currMaxItemsTracked} and ${size}")
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
