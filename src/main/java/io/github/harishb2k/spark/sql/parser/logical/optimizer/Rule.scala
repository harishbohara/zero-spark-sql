package io.github.harishb2k.spark.sql.parser.logical.optimizer

import io.github.harishb2k.spark.sql.parser.node._

import java.util.Objects

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

class PredicatePushDownUnresolvedScan extends Rule {
  /**
   * Apply a rule to logical plan
   */
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case u@UnresolvedScan(scanTableName) =>
      var root: LogicalPlan = u
      var parent: LogicalPlan = u
      while (root != null) {
        root = root.parent
        if (root != null) {
          parent = root;
        }
      }
      if (parent == null) {
        return plan
      }

      // Find where clause with matching table name
      val callback = new ITTraversal {
        override def callback(plan: LogicalPlan): LogicalPlan = {
          plan match {
            case w@UnresolvedWhere(tableName, filedName, operator) if Objects.equals(tableName, scanTableName) =>
              w
            case _ => null
          }
        }
      }
      val where: LogicalPlan = parent.travers(callback)

      if (where != null) {
        // us.parent.removeChildren(us)
        // where.addChildren(us)
        // where
      } else {
        // us
      }
      u
  }
}