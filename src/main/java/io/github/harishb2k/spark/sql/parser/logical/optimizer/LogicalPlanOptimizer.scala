package io.github.harishb2k.spark.sql.parser.logical.optimizer

import io.github.harishb2k.spark.sql.parser.node.LogicalPlan

import java.util
import scala.collection.JavaConversions._

class LogicalPlanOptimizer {

  val rulesV1 = new util.ArrayList[Rule]
  this.rulesV1.add(new WhereClauseCollapseRule)


  def optimize(logicalPlan: LogicalPlan): Any = {
    var lp = logicalPlan;
    for (rule <- rulesV1) {
      val afterApply = rule.apply(lp)

      if (afterApply == lp) {

      } else {

      }
    }
  }
}
