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

// import scala.reflect.runtime.universe._

import org.apache.datasketches.common._
import org.apache.datasketches.frequencies.ItemsSketch
import org.apache.datasketches.memory.Memory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, Literal}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types._


/**
 * Abstract class for ApproxTopKCombine
 */
abstract class AbsApproxTopKCom[T]
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

  //  def printType(): Unit = {
  //    // scalastyle:off
  //    println("T type: " + typeOf[T])
  //    // scalastyle:on
  //  }

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, IntegerType)

  override def createAggregationBuffer(): ItemsSketch[T] = new ItemsSketch[T](maxMapSize)

  override def merge(sketch: ItemsSketch[T], input: ItemsSketch[T]): ItemsSketch[T] =
    sketch.merge(input)

  override def serialize(sketch: ItemsSketch[T]): Array[Byte] = {
    // Need to know the actual type of T, cannot get from left or right
    sketch.toByteArray(new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
  }

  override def deserialize(buffer: Array[Byte]): ItemsSketch[T] = {
    ItemsSketch.getInstance(
      Memory.wrap(buffer), new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[T]])
  }

  override def update(sketch: ItemsSketch[T], input: InternalRow): ItemsSketch[T] = {
    val v = left.eval(input)
    if (v != null) {
      val inputSketch = ItemsSketch.getInstance(
          Memory.wrap(v.asInstanceOf[Array[Byte]]), new ArrayOfNumbersSerDe())
        .asInstanceOf[ItemsSketch[T]]
      sketch.merge(inputSketch)
    }
    sketch
  }
}

case class ApproxTopKCombine(
                              left: Expression,
                              right: Expression,
                              mutableAggBufferOffset: Int = 0,
                              inputAggBufferOffset: Int = 0)
  extends AbsApproxTopKCom[Any]
    with BinaryLike[Expression] {

  def this(child: Expression, maxItemsTracked: Expression) = {
    this(child, maxItemsTracked, 0, 0)
    //    this.printType()
  }

  def this(child: Expression, maxItemsTracked: Int) = {
    this(child, Literal(maxItemsTracked), 0, 0)
    //    this.printType()
  }

  def this(child: Expression) = {
    this(child, Literal(10000), 0, 0)
    //    this.printType()
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ApproxTopKCombine =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ApproxTopKCombine =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression):
  ApproxTopKCombine = copy(left = newLeft, right = newRight)

  override def nullable: Boolean = false

  override def prettyName: String = "approx_top_k_combine"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, IntegerType)

  override def dataType: DataType = BinaryType

  override def eval(sketch: ItemsSketch[Any]): Any = this.serialize(sketch)
}

