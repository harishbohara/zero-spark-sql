package io.github.harishb2k.spark.sql.parser.logical.optimizer

import io.github.harishb2k.spark.sql.parser.node.LogicalPlan

abstract class Rule {
  /**
   * Apply a rule to logical plan
   */
  def apply(plan: LogicalPlan): LogicalPlan
}
