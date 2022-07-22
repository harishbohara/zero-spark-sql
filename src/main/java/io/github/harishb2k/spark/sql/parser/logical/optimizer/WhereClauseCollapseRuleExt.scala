package io.github.harishb2k.spark.sql.parser.logical.optimizer

import io.github.harishb2k.spark.sql.parser.node.LogicalPlan


class WhereClauseCollapseRuleExt extends LogicalRule {
  override def transform(logicalPlan: LogicalPlan): LogicalPlan = {
    logicalPlan
  }
}
