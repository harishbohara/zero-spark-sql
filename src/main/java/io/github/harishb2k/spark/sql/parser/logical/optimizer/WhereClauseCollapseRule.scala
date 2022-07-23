package io.github.harishb2k.spark.sql.parser.logical.optimizer

import io.github.harishb2k.spark.sql.parser.node.LogicalPlan


class WhereClauseCollapseRule extends LogicalRule {
  override def transform(logicalPlan: LogicalPlan): LogicalPlan = {
    logicalPlan
  }
}
