package io.github.harishb2k.spark.sql.parser.node

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser
import org.apache.commons.lang3.StringUtils

import java.util
import scala.collection.JavaConversions._

abstract class LogicalPlan {

  var parent: LogicalPlan = _;

  val children = new util.ArrayList[LogicalPlan]

  /**
   * Add a children to this node
   */
  def addChildren(logicalPlan: LogicalPlan): Unit = {
    children.add(logicalPlan)
    logicalPlan.parent = this
  }

  def transform(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {

    val parent = this.parent

    // Apply partial function on this plan
    var afterRule = this
    if (rule.isDefinedAt(this)) {
      afterRule = rule.apply(this)
    }

    // Process children
    if (afterRule == this) {
      if (!children.isEmpty) {
        for (i <- 0 until children.size()) {
          val lp = children.get(i)
          lp.transform(rule)
        }
      }
    } else {

      // If this is a new logical plan - after running the rule than replace it
      if (parent != null) {
        parent.children.remove(this)
        parent.addChildren(afterRule);
      }

      for (i <- 0 until children.size()) {
        val lp = children.get(i)
        lp.transform(rule)
      }
    }

    afterRule
  }

  /** Describe this plan */
  def describe(verbose: Boolean): String = "LogicalPlan"

  /** String representation */
  override def toString: String = describe(false)

  /** Printer */
  def print(depth: Int): Unit = {
    val t = '\t'
    var s = ""
    if (describe(false) != null) s = describe(false)
    if (depth == 1) System.out.println(s)
    import scala.collection.JavaConversions._
    for (n <- children) {
      s = n.describe(false)
      if (n.describe(false) != null) s = n.describe(false)
      System.out.println(StringUtils.repeat(t, depth) + s)
      n.print(depth + 1)
    }
  }
}


/** This class is used to create a single select */
class UnresolvedSingleSelect extends LogicalPlan {
  var projection: UnresolvedProjection = _
  var join: Join = _
}

object UnresolvedSingleSelect {
  def unapply(lp: UnresolvedSingleSelect): Option[(UnresolvedProjection, Join)] = {
    Some(lp.projection, lp.join)
  }
}


/** This class is used to create a join which is not resolved select */
case class UnresolvedJoin(primaryRelationName: String, secondaryRelationName: String) extends LogicalPlan {
  override def describe(verbose: Boolean): String = "UnresolvedJoin: PrimaryRelation=" + primaryRelationName + " SecondaryRelation=" + secondaryRelationName
}

object UnresolvedJoin {
  def apply(ctx: SqlBaseParser.JoinRelationContext): UnresolvedJoin = {
    val primaryRelationName = ctx.parent.asInstanceOf[SqlBaseParser.RelationContext].relationPrimary.getText
    val secondaryRelationName = ctx.right.parent.asInstanceOf[SqlBaseParser.JoinRelationContext].relationPrimary.getText
    new UnresolvedJoin(primaryRelationName, secondaryRelationName)
  }
}


/** This class is used to create unresolved select projection */
case class UnresolvedProjection(columns: util.ArrayList[String]) extends LogicalPlan {
  override def describe(verbose: Boolean): String = "UnresolvedProjection: " + String.join(",", columns)
}

object UnresolvedProjection {
  def apply(ctx: SqlBaseParser.SelectClauseContext): UnresolvedProjection = {
    val columns = new util.ArrayList[String]
    for (n <- ctx.namedExpressionSeq.namedExpression) {
      columns.add(n.getText)
    }
    UnresolvedProjection(columns)
  }
}

/** Unresolved where clause */
case class UnresolvedWhere() extends LogicalPlan {
  override def describe(verbose: Boolean): String = "Where"
}

/** Unresolved where clause */
case class ResolvedWhere(tableName: String, filedName: String, operator: String) extends LogicalPlan {
  override def describe(verbose: Boolean): String = "ResolvedWhere: " + "table=" + tableName + " filed=" + filedName + " operator=" + operator
}

/** From  clause */
case class FromClause(tableName: String) extends LogicalPlan {
  override def describe(verbose: Boolean) = "FromClause: table=" + tableName
}

object FromClause {
  def unapply(f: LogicalPlan): Option[UnresolvedJoin] = {
    if (f.children.isEmpty) {
      None
    } else {
      f.children(0) match {
        case j: UnresolvedJoin => Some(j)
        case _ => None
      }
    }
  }
}

/** Capture comparison from where clause */
case class UnresolvedComparison(tableName: String, filedName: String, operator: String) extends LogicalPlan {
  override def describe(verbose: Boolean): String = "Comparison: " + "table=" + tableName + " filed=" + filedName + " operator=" + operator
}

case class UnresolvedScan(tableName: String) extends LogicalPlan {
  override def describe(verbose: Boolean): String = "UnresolvedScan: " + "table=" + tableName
}

case class Join() extends LogicalPlan {
  override def describe(verbose: Boolean): String = "Join"
}

object Join {
  def apply(left: UnresolvedScan, right: UnresolvedScan): Join = {
    val j = new Join
    j.addChildren(left)
    j.addChildren(right)
    j
  }
}