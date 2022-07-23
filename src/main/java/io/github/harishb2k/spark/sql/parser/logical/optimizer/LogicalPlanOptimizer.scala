package io.github.harishb2k.spark.sql.parser.logical.optimizer

import io.github.harishb2k.spark.sql.parser.node.{LogicalPlan, UnresolvedSingleSelect}

import java.util
import scala.collection.JavaConversions._

class LogicalPlanOptimizer {

  val rulesV1 = new util.ArrayList[Rule]
  this.rulesV1.add(new WhereClauseCollapseRule)
  this.rulesV1.add(new FromClauseRule)
  this.rulesV1.add(new MergeScanWithWhereClause)
  this.rulesV1.add(new MergeJoinAndProjection)


  def optimize(logicalPlan: LogicalPlan): LogicalPlan = {
    var lp = logicalPlan;
    for (rule <- rulesV1) {
      val afterApply = rule.apply(lp)

      if (afterApply.isInstanceOf[UnresolvedSingleSelect]) {
        lp = afterApply
      }

      if (afterApply == lp) {
      } else {
      }
    }
    lp
  }
}
