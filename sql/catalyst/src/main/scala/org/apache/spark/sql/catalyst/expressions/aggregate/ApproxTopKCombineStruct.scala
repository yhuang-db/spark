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

import org.apache.datasketches.frequencies.ItemsSketch

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType}

case class ApproxTopKCombineNew(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ItemsSketch[Int]]
  with BinaryLike[Expression]
  with ExpectsInputTypes {


  /**
   * Creates an empty aggregation buffer object. This is called before processing each key group
   * (group by key).
   *
   * @return an aggregation buffer object
   */
  override def createAggregationBuffer(): ItemsSketch[Int] = ???

  /**
   * Updates the aggregation buffer object with an input row and returns a new buffer object. For
   * performance, the function may do in-place update and return it instead of constructing new
   * buffer object.
   *
   * This is typically called when doing Partial or Complete mode aggregation.
   *
   * @param buffer The aggregation buffer object.
   * @param input  an input row
   */
  override def update(buffer: ItemsSketch[Int], input: InternalRow): ItemsSketch[Int] = ???

  /**
   * Merges an input aggregation object into aggregation buffer object and returns a new buffer
   * object. For performance, the function may do in-place merge and return it instead of
   * constructing new buffer object.
   *
   * This is typically called when doing PartialMerge or Final mode aggregation.
   *
   * @param buffer the aggregation buffer object used to store the aggregation result.
   * @param input  an input aggregation object. Input aggregation object can be produced by
   *               de-serializing the partial aggregate's output from Mapper side.
   */
  override def merge(buffer: ItemsSketch[Int], input: ItemsSketch[Int]): ItemsSketch[Int] = ???

  /**
   * Generates the final aggregation result value for current key group with the aggregation buffer
   * object.
   *
   * Developer note: the only return types accepted by Spark are:
   *   - primitive types
   *   - InternalRow and subclasses
   *   - ArrayData
   *   - MapData
   *
   * @param buffer aggregation buffer object.
   * @return The aggregation result of current key group
   */
  override def eval(buffer: ItemsSketch[Int]): Any = ???

  /** Serializes the aggregation buffer object T to Array[Byte] */
  override def serialize(buffer: ItemsSketch[Int]): Array[Byte] = ???

  /** De-serializes the serialized format Array[Byte], and produces aggregation buffer object T */
  override def deserialize(storageFormat: Array[Byte]): ItemsSketch[Int] = ???

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = ???

  /**
   * Expected input types from child expressions. The i-th position in the returned seq indicates
   * the type requirement for the i-th child.
   *
   * The possible values at each position are:
   * 1. a specific data type, e.g. LongType, StringType.
   * 2. a non-leaf abstract data type, e.g. NumericType, IntegralType, FractionalType.
   */
  override def inputTypes: Seq[AbstractDataType] = ???

  /**
   * Returns a copy of this ImperativeAggregate with an updated mutableAggBufferOffset.
   * This new copy's attributes may have different ids than the original.
   */
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate = ???

  /**
   * Returns a copy of this ImperativeAggregate with an updated mutableAggBufferOffset.
   * This new copy's attributes may have different ids than the original.
   */
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate = ???

  override def nullable: Boolean = ???

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  override def dataType: DataType = ???
}