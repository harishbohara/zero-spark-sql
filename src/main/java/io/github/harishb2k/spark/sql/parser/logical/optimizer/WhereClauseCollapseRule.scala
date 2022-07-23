package io.github.harishb2k.spark.sql.parser.logical.optimizer

import io.github.harishb2k.spark.sql.parser.node.{LogicalPlan, ResolvedWhere, UnresolvedComparison, UnresolvedWhere}

import scala.collection.JavaConversions._

class WhereClauseCollapseRule extends Rule {
  /**
   * Apply a rule to logical plan
   */
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case w: UnresolvedWhere => {

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
}
