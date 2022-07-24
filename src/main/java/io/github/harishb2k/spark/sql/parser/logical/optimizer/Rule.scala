package io.github.harishb2k.spark.sql.parser.logical.optimizer

import io.github.harishb2k.spark.sql.parser.node._

/**
 * A rule which runs on LogicalPlan and does some transformation on the plan.
 */
abstract class Rule {

  /**
   * Apply a rule to logical plan
   */
  def apply(plan: LogicalPlan): LogicalPlan


}

/**
 * This class looks for a "UnresolvedJoin" and converts it to "UnresolvedSimpleJoin with left and right scan"
 */
class UnresolvedJoinRule extends Rule {
  /**
   * Apply a rule to logical plan
   */
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case UnresolvedJoin(primaryRelationName, secondaryRelationName) =>
      val left = UnresolvedScan(primaryRelationName)
      val right = UnresolvedScan(secondaryRelationName)
      UnresolvedSimpleJoin(left, right)
  }
}

class UnresolvedFromClauseWithSingleTable extends Rule {
  /**
   * Apply a rule to logical plan
   */
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case UnresolvedFromClause(primaryTableName, simpleFromClause: Boolean) if simpleFromClause =>
      UnresolvedScan(primaryTableName)

    case ufc@UnresolvedFromClause(_, simpleFromClause: Boolean) if !simpleFromClause =>
      ufc.internalChildren.get(0)
  }
}