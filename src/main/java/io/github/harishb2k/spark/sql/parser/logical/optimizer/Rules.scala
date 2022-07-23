package io.github.harishb2k.spark.sql.parser.logical.optimizer

import io.github.harishb2k.spark.sql.parser.node._

import java.util.Objects
import scala.collection.JavaConversions._

// A rule which runs on LogicalPlan and does some transformation on the plan.
abstract class Rule {
  /**
   * Apply a rule to logical plan
   */
  def apply(plan: LogicalPlan): LogicalPlan
}


// This rule find a unresolved where clause and also looks for the comparison which is attached to that whare clause.
// Final result will be a resolved where clause
class WhereClauseCollapseRule extends Rule {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case w: UnresolvedWhere =>

      // If where clause has a child which is a comparison, then we will merge it with where clause
      for (lp <- w.children) {
        return lp match {
          case UnresolvedComparison(tableName, filedName, operator) => ResolvedWhere(tableName, filedName, operator)
          case _ => plan
        }
      }

      plan
  }
}

class FromClauseRule extends Rule {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case FromClause(unresolvedJoin: UnresolvedJoin) =>
      val left = UnresolvedScan(unresolvedJoin.primaryRelationName)
      val right = UnresolvedScan(unresolvedJoin.secondaryRelationName)
      Join(left, right)
  }
}

class MergeScanWithWhereClause extends Rule {
  /**
   * Apply a rule to logical plan
   */
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case s: UnresolvedScan if s.parent.isInstanceOf[ResolvedWhere] =>
      s
    case s: UnresolvedScan =>
      var ret: LogicalPlan = s
      for (i <- 0 until s.parent.parent.children.size()) {
        s.parent.parent.children.get(i) match {
          case where: ResolvedWhere if Objects.equals(where.tableName, s.tableName) =>
            ret = where
            ret.addChildren(s)
          case _ =>
        }
      }
      if (s != ret) s.parent.parent.children.remove(ret)
      ret
  }
}

class MergeJoinAndProjection extends Rule {
  /**
   * Apply a rule to logical plan
   */
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case us@UnresolvedSingleSelect(p, j) if j != null =>
      us

    case us@UnresolvedSingleSelect(p, j) =>
      for (n <- us.children) {
            n match {
              case _p: UnresolvedProjection => us.projection = _p
              case _j: Join => us.join = _j
              case _ =>
            }
      }

      if (us.join == null || us.projection == null) {
        return us
      }

      val newP = new UnresolvedProjection(us.projection.columns)
      newP.addChildren(us.join)
      val s = new UnresolvedSingleSelect
      s.addChildren(newP)
      s
  }
}