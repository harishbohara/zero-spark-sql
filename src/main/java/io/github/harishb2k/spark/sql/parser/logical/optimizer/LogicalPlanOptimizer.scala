package io.github.harishb2k.spark.sql.parser.logical.optimizer

import io.github.harishb2k.spark.sql.parser.node.LogicalPlan

import java.util
import scala.collection.JavaConversions._

class LogicalPlanOptimizer {
  val rules = new util.ArrayList[LogicalRule]
  this.rules.add(new WhereClauseCollapseRuleExt)


  def optimize(logicalPlan: LogicalPlan): Any = {
    for (rule <- rules) {
      rule.transform(logicalPlan)
    }
    for (n <- logicalPlan.children) {
      optimize(n)
    }
    logicalPlan
  }
}
