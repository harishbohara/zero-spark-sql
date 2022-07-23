package io.github.harishb2k.spark.sql.parser.logical.optimizer

import io.github.harishb2k.spark.sql.parser.node._

import java.util.Objects
import scala.collection.JavaConversions._

class Rules {
}


class WhereClauseCollapseRule extends Rule {
  /**
   * Apply a rule to logical plan
   */
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case w: UnresolvedWhere =>

      // If where clause has a child which is a comparison, then we will merge it with where clause
      for (lp <- w.children) {
        return lp match {
          case c: UnresolvedComparison =>
            return ResolvedWhere(c.tableName, c.filedName, c.operator)
          case _ => null
        }
      }

      // Noting matched - return same old plan
      plan
  }
}

class FromClauseRule extends Rule {
  /**
   * Apply a rule to logical plan
   */
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f: FromClause =>
      f.children(0) match {
        case j: UnresolvedJoin =>
          val left = UnresolvedScan(j.primaryRelationName)
          val right = UnresolvedScan(j.secondaryRelationName)
          Join(left, right)
        case _ => plan
      }
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