package io.github.harishb2k.spark.sql.parser.logical.optimizer

import io.github.harishb2k.spark.sql.parser.node.Node


class WhereClauseCollapseRuleExt extends LogicalRule {
  override def transform(node: Node): Node = {
    node
  }
}

case class XA() extends Node {
  override def canEqual(that: Any): Boolean = {
    true
  }
}

