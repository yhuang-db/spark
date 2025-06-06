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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.{HashMap, LinkedHashMap}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.analysis.{
  NondeterministicExpressionCollection,
  UnresolvedAttribute
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  Expression,
  ExprId,
  NamedExpression,
  SortOrder
}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project, Sort}

/**
 * Resolves a [[Sort]] by resolving its child and order expressions.
 */
class SortResolver(operatorResolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[Sort, LogicalPlan]
    with ResolvesNameByHiddenOutput {
  private val scopes: NameScopeStack = operatorResolver.getNameScopes
  private val autoGeneratedAliasProvider = new AutoGeneratedAliasProvider(
    expressionResolver.getExpressionIdAssigner
  )

  /**
   * Resolve [[Sort]] operator.
   *
   *  1. Resolve [[Sort.child]] and set [[NameScope.ordinalReplacementExpressions]] for
   *     [[SortOrder]] resolution.
   *  2. Resolve order expressions using [[ExpressionResolver.resolveExpressionTreeInOperator]].
   *  3. In case order expressions contain only one element, `UnresolvedAttribute(ALL)`, which
   *     can't be resolved from current (nor from hidden output) - this is done using
   *     [[ResolveAsAllKeyword]], skip previous step and resolve it as an `ALL` keyword (by copying
   *     child's output and transforming it into attributes).
   *  4. In case there are attributes referenced in the order expressions are resolved using
   *     the hidden output (or in case we order by [[AggregateExpression]]s which are not present
   *     in [[Aggregate.aggregateExpressions]]) update the output of child operator and add a
   *     [[Project]] as a parent of [[Sort]] with original [[Project]]'s output (this is done by
   *     [[ResolvesNameByHiddenOutput]]). Query:
   *     {{{
   *     SELECT col1 FROM VALUES(1, 2) WHERE col2 > 2 ORDER BY col2;
   *     }}}
   *     Has the following unresolved plan:
   *
   *     'Sort ['col2 ASC NULLS FIRST], true
   *       +- 'Project ['col1]
   *         +- 'Filter ('col2 > 2)
   *           +- LocalRelation [col1#92225, col2#92226]
   *
   *     Because `col2` from the [[Sort]] node is resolved using the hidden output, add it to the
   *     [[Project.projectList]] and add a [[Project]] with original project list as a top node:
   *
   *     Project [col1]
   *       +- Sort [col2 ASC NULLS FIRST], true
   *         +- Project [col1, col2]
   *           +- Filter (col2 > 2)
   *             +- LocalRelation [col1, col2]
   *
   *     Another example with ordering by [[AggregateExpression]]:
   *     {{{
   *     SELECT col1 FROM VALUES (1, 2) GROUP BY col1, col2 + 1 ORDER BY SUM(col1), col2 + 1;
   *     }}}
   *     Has the following unresolved plan:
   *
   *     'Sort ['SUM('col1) ASC NULLS FIRST, ('col2 + 1) ASC NULLS FIRST], true
   *       +- 'Aggregate ['col1, ('col2 + 1)], ['col1]
   *         +- LocalRelation [col1, col2]
   *
   *     Because neither `SUM(col1)` nor `col2 + 1` from the [[Sort]] node are present in the
   *     [[Aggregate.aggregateExpressions]], add them to it and add a [[Project]] with original
   *     project list as a top node (`SUM(col2)` is extracted in the
   *     [[AggregateExpressionResolver]] whereas `col2 + 1` is extracted using
   *     `extractReferencedGroupingAndAggregateExpressions` helper method):
   *
   *     Project [col1]
   *       +- Sort [sum(col1)#... ASC NULLS FIRST, (col2 + 1)#... ASC NULLS FIRST], true
   *         +- Aggregate [col1, (col2 + 1)],
   *                      [col1, sum(col1) AS sum(col1)#..., (col2 + 1) AS (col2 + 1)#...]
   *           +- LocalRelation [col1, col2]
   *  5. In case there are non-deterministic expressions in the order expressions, substitute them
   *     with derived attribute references to an artificial [[Project]] list.
   */
  override def resolve(unresolvedSort: Sort): LogicalPlan = {
    val resolvedChild = operatorResolver.resolve(unresolvedSort.child)

    scopes.current.setOrdinalReplacementExpressions(
      OrdinalReplacementSortOrderExpressions(
        expressions = scopes.current.output.toIndexedSeq,
        unresolvedSort = unresolvedSort
      )
    )

    if (canOrderByAll(unresolvedSort.order)) {
      val sortOrder = unresolvedSort.order.head
      val resolvedOrder =
        scopes.current.output.map(a => sortOrder.copy(child = a.toAttribute))
      unresolvedSort.copy(child = resolvedChild, order = resolvedOrder)
    } else {
      val partiallyResolvedSort = unresolvedSort.copy(child = resolvedChild)

      val (resolvedOrderExpressions, missingAttributes) =
        resolveOrderExpressions(partiallyResolvedSort)

      val (finalOrderExpressions, missingExpressions) = resolvedChild match {
        case _ if scopes.current.hasLcaInAggregate =>
          throw new ExplicitlyUnsupportedResolverFeature(
            "Lateral column alias in Aggregate below a Sort"
          )
        case aggregate: Aggregate =>
          val (cleanedOrderExpressions, extractedExpressions) =
            extractReferencedGroupingAndAggregateExpressions(aggregate, resolvedOrderExpressions)
          (cleanedOrderExpressions, extractedExpressions)
        case filter @ Filter(_, aggregate: Aggregate) =>
          val (cleanedOrderExpressions, extractedExpressions) =
            extractReferencedGroupingAndAggregateExpressions(aggregate, resolvedOrderExpressions)
          (cleanedOrderExpressions, extractedExpressions)
        case project @ Project(_, Filter(_, aggregate: Aggregate)) =>
          throw new ExplicitlyUnsupportedResolverFeature(
            "Project on top of HAVING below a Sort"
          )
        case other =>
          (resolvedOrderExpressions, missingAttributes)
      }

      val resolvedChildWithMissingAttributes =
        insertMissingExpressions(resolvedChild, missingExpressions)

      val resolvedSort = unresolvedSort.copy(
        child = resolvedChildWithMissingAttributes,
        order = finalOrderExpressions
      )

      val sortWithOriginalOutput = retainOriginalOutput(
        operator = resolvedSort,
        missingExpressions = missingExpressions,
        output = scopes.current.output,
        hiddenOutput = scopes.current.hiddenOutput
      )

      sortWithOriginalOutput match {
        case project @ Project(_, sort: Sort) =>
          project.copy(child = tryPullOutNondeterministic(sort, childOutput = sort.child.output))
        case sort: Sort =>
          tryPullOutNondeterministic(sort, childOutput = scopes.current.output)
      }
    }
  }

  /**
   * Resolve order expressions of an unresolved [[Sort]], returns attributes resolved using hidden
   * output and extracted [[AggregateExpression]]s. In case of [[UnresolvedAttribute]] resolution,
   * respect the following order.
   *
   *  1. Attribute can be resolved using the current scope:
   *     {{{
   *     -- This one will resolve `col1` from the current scope (its value will be 1)
   *     SELECT col1 FROM VALUES(1, 2) WHERE (SELECT col1 FROM VALUES(3)) ORDER BY col1;
   *     }}}
   *
   *  2. Attribute can be resolved using the hidden output. Attribute is added to
   *     `missingAttributes` which is used to update the plan using the
   *     [[ResolvesNameByHiddenOutput]].
   *     {{{
   *     -- This one will resolve `col2` from hidden output
   *     SELECT col1 FROM VALUES(1, 2) WHERE col2 > 2 ORDER BY col2;
   *     }}}
   *
   *  3. In case attribute can't be resolved from output nor from hidden output, throw
   *     `UNRESOLVED_COLUMN` exception:
   *     {{{
   *     -- Following queries throw `UNRESOLVED_COLUMN` exception:
   *     SELECT col1 FROM VALUES(1,2) GROUP BY col1 HAVING col1 > 1 ORDER BY col2;
   *     SELECT col1 FROM VALUES(1) ORDER BY col2;
   *     }}}
   *
   * If the order expression is not present in the current scope, but an alias of this expression
   * is, replace the order expression with its alias (see
   * [[tryReplaceSortOrderExpressionWithAlias]]).
   */
  private def resolveOrderExpressions(
      partiallyResolvedSort: Sort): (Seq[SortOrder], Seq[Attribute]) = {
    val referencedAttributes = new HashMap[ExprId, Attribute]

    val resolvedSortOrder = partiallyResolvedSort.order.map { sortOrder =>
      val resolvedSortOrder = expressionResolver
        .resolveExpressionTreeInOperator(sortOrder, partiallyResolvedSort)
        .asInstanceOf[SortOrder]

      tryReplaceSortOrderExpressionWithAlias(resolvedSortOrder).getOrElse {
        referencedAttributes.putAll(expressionResolver.getLastReferencedAttributes)

        resolvedSortOrder
      }
    }

    val missingAttributes = scopes.current.resolveMissingAttributesByHiddenOutput(
      referencedAttributes
    )

    (resolvedSortOrder, missingAttributes)
  }

  /**
   * When resolving [[SortOrder]] on top of an [[Aggregate]], if there is an attribute that is
   * present in `hiddenOutput` and there is an [[Alias]] of this attribute in the `output`,
   * [[SortOrder]] should be resolved by the [[Alias]] instead of an attribute. This is done as
   * optimization in order to avoid a [[Project]] node being added when resolving the attribute via
   * missing input (because attribute is not present in direct output, only its alias is).
   *
   * For example, for a query like:
   *
   * {{{
   * SELECT col1 + 1 AS a FROM VALUES(1) GROUP BY a ORDER BY col1 + 1;
   * }}}
   *
   * The resolved plan should be:
   *
   * Sort [a#2 ASC NULLS FIRST], true
   * +- Aggregate [(col1#1 + 1)], [(col1#1 + 1) AS a#2]
   *    +- LocalRelation [col1#1]
   *
   * [[SortOrder]] expression is resolved to alias of `col1 + 1` instead of `col1 + 1` itself.
   */
  private def tryReplaceSortOrderExpressionWithAlias(sortOrder: SortOrder): Option[SortOrder] = {
    scopes.current.aggregateListAliases
      .collectFirst {
        case alias if alias.child.semanticEquals(sortOrder.child) => alias.toAttribute
      }
      .map { aliasCandidate =>
        sortOrder.withNewChildren(newChildren = Seq(aliasCandidate)).asInstanceOf[SortOrder]
      }
  }

  /**
   * Extracts the referenced grouping and aggregate expressions from the order expressions. This is
   * used to update the output of the child operator and add a [[Project]] as a parent of [[Sort]]
   * later during the resolution (if needed). Consider the following example:
   * {{{
   * SELECT col1 FROM VALUES (1, 2) GROUP BY col1, col2 ORDER BY col2;
   * }}}
   *
   * The unresolved plan would look like this:
   *
   * 'Sort ['col2 ASC NULLS FIRST], true
   *   +- 'Aggregate ['col1, 'col2], ['col1]
   *     +- LocalRelation [col1, col2]
   *
   * As it can be seen, `col2` (ordering expression) is not present in the [[Aggregate]] operator
   * , and thus we return it from this method. The plan will be altered later during the resolution
   * using the [[ResolvesNameByHiddenOutput]] (`col2` will be added to
   * [[Aggregate.aggregateExpressions]], [[Project]] will be added as a top node with original
   * [[Aggregate]] output) and it will look like:
   *
   * Project [col1]
   *   +- Sort [col2 ASC NULLS FIRST], true
   *     +- Aggregate [col1, col2], [col1, col2]
   *       +- LocalRelation [col1, col2]
   *
   * Extraction is done in a top-down manner by traversing the expression tree of the condition,
   * swapping an underlying expression found in the grouping or aggregate expressions with the one
   * that matches it and populating the `referencedGroupingExpressions` and
   * `extractedAggregateExpressionAliases` lists to insert missing expressions later.
   */
  private def extractReferencedGroupingAndAggregateExpressions(
      aggregate: Aggregate,
      sortOrderEntries: Seq[SortOrder]): (Seq[SortOrder], Seq[NamedExpression]) = {
    val groupingAndAggregateExpressionsExtractor =
      new GroupingAndAggregateExpressionsExtractor(aggregate, autoGeneratedAliasProvider)

    val referencedGroupingExpressions = new mutable.ArrayBuffer[NamedExpression]
    val extractedAggregateExpressionAliases = new mutable.ArrayBuffer[Alias]

    val transformedSortOrderEntries = sortOrderEntries.map { sortOrder =>
      sortOrder.copy(child = sortOrder.child.transformDown {
        case expression: Expression =>
          groupingAndAggregateExpressionsExtractor.extractReferencedGroupingAndAggregateExpressions(
            expression = expression,
            referencedGroupingExpressions = referencedGroupingExpressions,
            extractedAggregateExpressionAliases = extractedAggregateExpressionAliases
          )
      })
    }

    (
      transformedSortOrderEntries,
      referencedGroupingExpressions.toSeq ++ extractedAggregateExpressionAliases.toSeq
    )
  }

  /**
   * In case there are non-deterministic expressions in `order` expressions replace them with
   * attributes created out of corresponding non-deterministic expression. Example:
   *
   * {{{ SELECT 1 ORDER BY RAND(); }}}
   *
   * This query would have the following analyzed plan:
   *
   * Project [1]
   *   +- Sort [_nondeterministic ASC NULLS FIRST], true
   *     +- Project [1, rand(...) AS _nondeterministic#...]
   *       +- Project [1 AS 1#...]
   *         +- OneRowRelation
   *
   * We use `childOutput` instead of directly calling `scopes.current.output`, because
   * [[insertMissingExpressions]] could have changed the output of the child operator.
   * We could just call `sort.child.output`, but this is suboptimal for the simple case when
   * [[Sort]] child is left unchanged, and in that case we actually call `scopes.current.output`.
   * See the call site in [[resolve]].
   */
  private def tryPullOutNondeterministic(sort: Sort, childOutput: Seq[Attribute]): LogicalPlan = {
    val nondeterministicToAttributes: LinkedHashMap[Expression, NamedExpression] =
      NondeterministicExpressionCollection.getNondeterministicToAttributes(
        sort.order.map(_.child)
      )

    if (!nondeterministicToAttributes.isEmpty) {
      val newChild = Project(
        childOutput ++ nondeterministicToAttributes.values.asScala.toSeq,
        sort.child
      )
      val resolvedOrder = sort.order.map { sortOrder =>
        sortOrder.copy(
          child = PullOutNondeterministicExpressionInExpressionTree(
            sortOrder.child,
            nondeterministicToAttributes
          )
        )
      }
      val resolvedSort = sort.copy(
        order = resolvedOrder,
        child = newChild
      )
      Project(projectList = childOutput, child = resolvedSort)
    } else {
      sort
    }
  }

  private def canOrderByAll(expressions: Seq[SortOrder]): Boolean = {
    val isOrderByAll = expressions match {
      case Seq(SortOrder(unresolvedAttribute: UnresolvedAttribute, _, _, _)) =>
        unresolvedAttribute.equalsIgnoreCase("ALL")
      case _ => false
    }
    isOrderByAll && scopes.current
      .resolveMultipartName(Seq("ALL"), canResolveNameByHiddenOutput = true)
      .candidates
      .isEmpty
  }
}
