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

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{ArrayOfDecimalsSerDe, BinaryExpression, Expression, ImplicitCastInputTypes, Literal, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.trees.{BinaryLike, TernaryLike}
import org.apache.spark.sql.catalyst.util.{CollationFactory, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


case class ApproxTopK(
    first: Expression,
    second: Expression,
    third: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ItemsSketch[Any]]
  with ImplicitCastInputTypes
  with TernaryLike[Expression] {

  def this(child: Expression, topK: Expression, maxItemsTracked: Expression) =
    this(child, topK, maxItemsTracked, 0, 0)

  def this(child: Expression, topK: Int, maxItemsTracked: Int) =
    this(child, Literal(topK), Literal(maxItemsTracked), 0, 0)

  def this(child: Expression, topK: Expression) =
    this(child, topK, Literal(ApproxTopK.DEFAULT_MAX_ITEMS_TRACKED), 0, 0)

  def this(child: Expression, topK: Int) =
    this(child, Literal(topK), Literal(ApproxTopK.DEFAULT_MAX_ITEMS_TRACKED), 0, 0)

  def this(child: Expression) =
    this(child, Literal(ApproxTopK.DEFAULT_K), Literal(ApproxTopK.DEFAULT_MAX_ITEMS_TRACKED), 0, 0)

  private lazy val itemDataType: DataType = first.dataType
  private lazy val k: Int = second.eval().asInstanceOf[Int]
  private lazy val numTracked: Int = third.eval().asInstanceOf[Int]

  // check data values
  if (!second.isInstanceOf[Unevaluable]) {
    require(second.eval() != null, "K cannot be NULL")
    require(k > 0, "K must be greater than 0")
  }
  if (!second.isInstanceOf[Unevaluable]) {
    require(third.eval() != null, "Number of items tracked cannot be NULL")
    require(numTracked > 0, "Number of items tracked must be greater than 0")
  }
  if (!second.isInstanceOf[Unevaluable] && !third.isInstanceOf[Unevaluable]) {
    require(k <= numTracked,
      s"K ($k) must be less than or equal to the number of items tracked ($numTracked)")
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegerType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!ApproxTopK.checkItemType(itemDataType)) {
      TypeCheckFailure(f"${itemDataType.typeName} columns are not supported")
    } else if (!second.foldable) {
      TypeCheckFailure("K must be a constant literal")
    } else if (!third.foldable) {
      TypeCheckFailure("Number of items tracked must be a constant literal")
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = ApproxTopK.getResultDataType(itemDataType)

  override def createAggregationBuffer(): ItemsSketch[Any] = {
    val maxMapSize = ApproxTopK.calMaxMapSize(numTracked)
    ApproxTopK.createAggregationBuffer(first, maxMapSize)
  }

  override def update(buffer: ItemsSketch[Any], input: InternalRow): ItemsSketch[Any] =
    ApproxTopK.updateSketchBuffer(first, buffer, input)

  override def merge(buffer: ItemsSketch[Any], input: ItemsSketch[Any]): ItemsSketch[Any] =
    buffer.merge(input)

  override def eval(buffer: ItemsSketch[Any]): Any = {
    ApproxTopK.genEvalResult(buffer, k, itemDataType)
  }

  override def serialize(buffer: ItemsSketch[Any]): Array[Byte] =
    buffer.toByteArray(ApproxTopK.genSketchSerDe(itemDataType))

  override def deserialize(storageFormat: Array[Byte]): ItemsSketch[Any] =
    ItemsSketch.getInstance(Memory.wrap(storageFormat), ApproxTopK.genSketchSerDe(itemDataType))

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): Expression =
    copy(first = newFirst, second = newSecond, third = newThird)

  override def nullable: Boolean = false

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("approx_top_k")
}

object ApproxTopK {

  val DEFAULT_K: Int = 5
  val DEFAULT_MAX_ITEMS_TRACKED: Int = 10000
  val VOID_MAX_ITEMS_TRACKED: Int = -1
  val SKETCH_SIZE_PLACEHOLDER: Int = 8

  def getResultDataType(itemDataType: DataType): DataType = {
    val resultEntryType = StructType(
      StructField("Item", itemDataType, nullable = false) ::
        StructField("Estimate", LongType, nullable = false) :: Nil)
    ArrayType(resultEntryType, containsNull = false)
  }

  def getSketchStateDataType(itemDataType: DataType): StructType =
    StructType(
      StructField("DataSketch", BinaryType, nullable = false) ::
        StructField("ItemTypeNull", itemDataType) ::
        StructField("MaxItemsTracked", IntegerType, nullable = false) :: Nil)


  def checkItemType(itemType: DataType): Boolean =
    itemType match {
      case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType |
           _: LongType | _: FloatType | _: DoubleType | _: DateType |
           _: TimestampType | _: StringType | _: DecimalType => true
      case _ => false
    }

  def calMaxMapSize(maxItemsTracked: Int): Int = {
    // The maximum capacity of this internal hash map is * 0.75 times * maxMapSize.
    val ceilMaxMapSize = math.ceil(maxItemsTracked / 0.75).toInt
    // The maxMapSize must be a power of 2 and greater than ceilMaxMapSize
    math.pow(2, math.ceil(math.log(ceilMaxMapSize) / math.log(2))).toInt
  }

  def createAggregationBuffer(itemExpression: Expression, maxMapSize: Int): ItemsSketch[Any] = {
    itemExpression.dataType match {
      case _: BooleanType =>
        new ItemsSketch[Boolean](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: ByteType | _: ShortType | _: IntegerType | _: FloatType | _: DateType =>
        new ItemsSketch[Number](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: LongType | _: TimestampType =>
        new ItemsSketch[Long](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: DoubleType =>
        new ItemsSketch[Double](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: StringType =>
        new ItemsSketch[String](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: DecimalType =>
        new ItemsSketch[Decimal](maxMapSize).asInstanceOf[ItemsSketch[Any]]
    }
  }

  def updateSketchBuffer(
      itemExpression: Expression,
      buffer: ItemsSketch[Any],
      input: InternalRow): ItemsSketch[Any] = {
    val v = itemExpression.eval(input)
    if (v != null) {
      itemExpression.dataType match {
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

  def genEvalResult(itemsSketch: ItemsSketch[Any], k: Int, itemDataType: DataType): Any = {
    val items = itemsSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES)
    val resultLength = math.min(items.length, k)
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

  def genSketchSerDe(dataType: DataType): ArrayOfItemsSerDe[Any] = {
    dataType match {
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
    }
  }
}

case class ApproxTopKAccumulate(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ItemsSketch[Any]]
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {

  def this(child: Expression, maxItemsTracked: Expression) = this(child, maxItemsTracked, 0, 0)

  def this(child: Expression, maxItemsTracked: Int) = this(child, Literal(maxItemsTracked), 0, 0)

  def this(child: Expression) = this(child, Literal(ApproxTopK.DEFAULT_MAX_ITEMS_TRACKED), 0, 0)

  private lazy val numTracked: Int = right.eval().asInstanceOf[Int]
  private lazy val itemDataType: DataType = left.dataType

  // check data values
  if (!right.isInstanceOf[Unevaluable]) {
    require(right.eval() != null, "Number of items tracked cannot be NULL")
    require(numTracked > 0, "Number of items tracked must be greater than 0")
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!ApproxTopK.checkItemType(itemDataType)) {
      TypeCheckFailure(f"${itemDataType.typeName} columns are not supported")
    } else if (!right.foldable) {
      TypeCheckFailure("Number of items tracked must be a constant literal")
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = ApproxTopK.getSketchStateDataType(itemDataType)

  override def createAggregationBuffer(): ItemsSketch[Any] = {
    val maxMapSize = ApproxTopK.calMaxMapSize(numTracked)
    ApproxTopK.createAggregationBuffer(left, maxMapSize)
  }

  override def update(buffer: ItemsSketch[Any], input: InternalRow): ItemsSketch[Any] =
    ApproxTopK.updateSketchBuffer(left, buffer, input)

  override def merge(buffer: ItemsSketch[Any], input: ItemsSketch[Any]): ItemsSketch[Any] =
    buffer.merge(input)

  override def eval(buffer: ItemsSketch[Any]): Any = {
    val sketchBytes = serialize(buffer)
    InternalRow.apply(sketchBytes, null, numTracked)
  }

  override def serialize(buffer: ItemsSketch[Any]): Array[Byte] =
    buffer.toByteArray(ApproxTopK.genSketchSerDe(itemDataType))

  override def deserialize(storageFormat: Array[Byte]): ItemsSketch[Any] =
    ItemsSketch.getInstance(Memory.wrap(storageFormat), ApproxTopK.genSketchSerDe(itemDataType))

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)

  override def nullable: Boolean = false

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("approx_top_k_accumulate")
}


case class ApproxTopKEstimate(left: Expression, right: Expression)
  extends BinaryExpression
  with CodegenFallback
  with ImplicitCastInputTypes {

  def this(child: Expression, topK: Int) = this(child, Literal(topK))

  def this(child: Expression) = this(child, Literal(ApproxTopK.DEFAULT_K))

  private lazy val itemDataType: DataType = {
    // itemDataType is the type of the "ItemTypeNull" field of the output of ACCUMULATE or COMBINE
    left.dataType.asInstanceOf[StructType]("ItemTypeNull").dataType
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StructType, IntegerType)

  override def dataType: DataType = ApproxTopK.getResultDataType(itemDataType)

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val dataSketchBytes = input1.asInstanceOf[InternalRow].getBinary(0)
    val topK = input2.asInstanceOf[Int]
    val itemsSketch = ItemsSketch.getInstance(
      Memory.wrap(dataSketchBytes), ApproxTopK.genSketchSerDe(itemDataType))
    ApproxTopK.genEvalResult(itemsSketch, topK, itemDataType)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)

  override def nullIntolerant: Boolean = true

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("approx_top_k_estimate")
}

class CombineInternal[T](sketch: ItemsSketch[T], itemDataType: DataType, var maxItemsTracked: Int) {
  def getSketch: ItemsSketch[T] = sketch

  def getItemDataType: DataType = itemDataType

  def getMaxItemsTracked: Int = maxItemsTracked

  def setMaxItemsTracked(maxItemsTracked: Int): Unit = this.maxItemsTracked = maxItemsTracked
}

case class ApproxTopKCombine(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[CombineInternal[Any]]
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {

  def this(child: Expression, maxItemsTracked: Expression) = this(child, maxItemsTracked, 0, 0)

  def this(child: Expression, maxItemsTracked: Int) = this(child, Literal(maxItemsTracked), 0, 0)

  def this(child: Expression) = this(child, Literal(ApproxTopK.VOID_MAX_ITEMS_TRACKED), 0, 0)

  private lazy val itemDataType: DataType =
    left.dataType.asInstanceOf[StructType]("ItemTypeNull").dataType
  private lazy val combineSizeSpecified: Boolean =
    right.eval().asInstanceOf[Int] != ApproxTopK.VOID_MAX_ITEMS_TRACKED

  override def inputTypes: Seq[AbstractDataType] = Seq(StructType, IntegerType)

  override def dataType: DataType = ApproxTopK.getSketchStateDataType(itemDataType)

  override def createAggregationBuffer(): CombineInternal[Any] = {
    if (combineSizeSpecified) {
      val maxItemsTracked = right.eval().asInstanceOf[Int]
      val maxMapSize = ApproxTopK.calMaxMapSize(maxItemsTracked)
      new CombineInternal[Any](new ItemsSketch[Any](maxMapSize), itemDataType, maxItemsTracked)
    } else {
      new CombineInternal[Any](
        new ItemsSketch[Any](ApproxTopK.SKETCH_SIZE_PLACEHOLDER),
        itemDataType,
        ApproxTopK.VOID_MAX_ITEMS_TRACKED)
    }
  }

  override def update(buffer: CombineInternal[Any], input: InternalRow): CombineInternal[Any] = {
    val inputSketchBytes = left.eval(input).asInstanceOf[InternalRow].getBinary(0)
    val inputMaxItemsTracked = left.eval(input).asInstanceOf[InternalRow].getInt(2)
    val inputSketch = try {
      ItemsSketch.getInstance(
        Memory.wrap(inputSketchBytes), ApproxTopK.genSketchSerDe(buffer.getItemDataType))
    } catch {
      case _: SketchesArgumentException | _: NumberFormatException =>
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

  override def merge(buffer: CombineInternal[Any], input: CombineInternal[Any])
  : CombineInternal[Any] = {
    if (!combineSizeSpecified) {
      // check size
      if (buffer.getMaxItemsTracked == ApproxTopK.VOID_MAX_ITEMS_TRACKED) {
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

  override def eval(buffer: CombineInternal[Any]): Any = {
    val sketchBytes = try {
      buffer.getSketch.toByteArray(ApproxTopK.genSketchSerDe(buffer.getItemDataType))
    } catch {
      case _: ArrayStoreException =>
        throw new SparkUnsupportedOperationException(
          errorClass = "APPROX_TOP_K_SKETCH_TYPE_UNMATCHED"
        )
    }
    val maxItemsTracked = buffer.getMaxItemsTracked
    InternalRow.apply(sketchBytes, null, maxItemsTracked)
  }

  override def serialize(buffer: CombineInternal[Any]): Array[Byte] = {
    val sketchBytes = buffer.getSketch.toByteArray(
      ApproxTopK.genSketchSerDe(buffer.getItemDataType))
    val maxItemsTrackedByte = buffer.getMaxItemsTracked.toByte
    val byteArray = new Array[Byte](sketchBytes.length + 1)
    byteArray(0) = maxItemsTrackedByte
    System.arraycopy(sketchBytes, 0, byteArray, 1, sketchBytes.length)
    byteArray
  }

  override def deserialize(buffer: Array[Byte]): CombineInternal[Any] = {
    val maxItemsTracked = buffer(0).toInt
    val sketchBytes = buffer.slice(1, buffer.length)
    val sketch = ItemsSketch.getInstance(
      Memory.wrap(sketchBytes), ApproxTopK.genSketchSerDe(itemDataType))
    new CombineInternal[Any](sketch, itemDataType, maxItemsTracked)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = copy(left = newLeft, right = newRight)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("approx_top_k_combine")
}
