package io.github.harishb2k.spark.sql.parser.logical.optimizer

import io.github.harishb2k.spark.sql.parser.node.{LogicalPlan, UnresolvedSingleSelect}

import java.util
import scala.collection.JavaConversions._

class LogicalPlanOptimizer {
  val rules = new util.ArrayList[Rule]
  rules.add(new UnresolvedJoinRule)

  def optimize(logicalPlan: LogicalPlan): LogicalPlan = {
    var lp = logicalPlan;

    // Run all rules in in input
    for (rule <- rules) {

      // Run a rule on input plan
      val afterApply = rule.apply(lp)

      // UnresolvedSingleSelect is the final result
      if (afterApply.isInstanceOf[UnresolvedSingleSelect]) {
        lp = afterApply
      }
    }

    // return updated plan
    lp
  }
}
