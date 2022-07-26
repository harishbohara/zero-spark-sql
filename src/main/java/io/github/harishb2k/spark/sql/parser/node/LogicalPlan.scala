package io.github.harishb2k.spark.sql.parser.node

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser
import org.apache.commons.lang3.StringUtils

import java.util
import scala.collection.JavaConversions._

/**
 * The parent node - I have named it <code>LogicalPlan</code> because this code is inspired from Spark code base.
 */
abstract class LogicalPlan {

  /**
   * Parent of this plan
   */
  var parent: LogicalPlan = _;

   val internalChildren = new util.ArrayList[LogicalPlan]

  /**
   * This takes a rule and transform this to new plan
   */
  def transform(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    val parent = this.parent

    // Apply partial function on this plan
    var afterRule = this
    if (rule.isDefinedAt(this)) {
      afterRule = rule.apply(this)
    }

    // See if new rule is changed then replace it
    if (afterRule == this) {
      if (!internalChildren.isEmpty) {
        for (i <- 0 until internalChildren.size()) {
          val lp = internalChildren.get(i)
          lp.transform(rule)
        }
      }
    } else {
      replaceChildren(this, afterRule)
    }

    afterRule
  }

  /**
   * Add a children to this node
   */
  def addChildren(logicalPlan: LogicalPlan): Unit = {
    internalChildren.add(logicalPlan)
    logicalPlan.parent = this
  }

  /**
   * Remove a children
   */
  def removeChildren(logicalPlan: LogicalPlan): Unit = {
    internalChildren.remove(logicalPlan)
    logicalPlan.parent = this
  }

  def detachFromParent(): Unit = {
    if (parent != null) {
      parent.removeChildren(this)
    }
  }

  /**
   * Remove a children and replace it with new
   */
  def replaceChildren(oldChildren: LogicalPlan, newChildren: LogicalPlan): Unit = {
    val parent = oldChildren.parent
    if (parent != null) {
      parent.removeChildren(oldChildren)
      parent.addChildren(newChildren)
    }
  }

  def moveToBecomeParentOf(child: LogicalPlan): LogicalPlan = {
    val childParent = child.parent
    if (childParent == parent) {
      childParent.removeChildren(child)
      addChildren(child)
    } else {
      detachFromParent()
      child.detachFromParent()
      addChildren(child)
      childParent.addChildren(this)
    }
    child
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

    for (n <- internalChildren) {
      s = n.describe(false)
      if (n.describe(false) != null) s = n.describe(false)
      System.out.println(StringUtils.repeat(t, depth) + s)
      n.print(depth + 1)
    }
  }

  def travers(callback: ITTraversal): LogicalPlan = {
    if (callback.callback(this) != null) {
      return this
    }

    if (!internalChildren.isEmpty) {
      for (n <- internalChildren) {
        val lp = n.travers(callback)
        if (lp != null) {
          return lp
        }
      }
    }

    null
  }
}


/** This class is used to create a parent select - it holds the main part of a select statment
 * Projection
 * Where
 * From
 * Order By
 * ...
 * */
class UnresolvedSingleSelect extends LogicalPlan {
  var projection: UnresolvedProjection = _
  var from: UnresolvedFromClause = _
  var where: UnresolvedWhere = _

  override def describe(verbose: Boolean): String = "UnresolvedSingleSelect: "
}

/** Unresolved columns names (x, y, z) from "Select x, y, x FROM table */
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

/** From  clause */
class UnresolvedFromClause(val tableName: String) extends LogicalPlan {
  override def describe(verbose: Boolean): String = "UnresolvedFromClause: table=" + tableName
}

object UnresolvedFromClause {
  def unapply(ufc: UnresolvedFromClause): Option[(String, Boolean)] = {
    if (ufc.internalChildren.isEmpty) {
      Some(ufc.tableName, true)
    } else {
      Some(ufc.tableName, false)
    }
  }
}

/** This class is used to create a join which is not resolved select */
case class UnresolvedJoin(primaryRelationName: String, secondaryRelationName: String) extends LogicalPlan {
  override def describe(verbose: Boolean): String = "UnresolvedJoin: PrimaryRelation=" + primaryRelationName + " SecondaryRelation=" + secondaryRelationName
}

object UnresolvedJoin {

  // If we have a join in a SQL then we will pick both the tables:
  // Primary   - main_table
  // Secondary - other_table
  //
  // Why - when we execute a query, we will read these 2 tables (may be from flat file) to join.
  //
  // SELECT `id`, `name`, `created_at`
  //                 FROM `main_table` INNER JOIN `other_table`
  //                    ON `main_table`.`column_name_main` = `other_table`.`column_name_other`
  //                 WHERE `main_table`.`id` > 10;
  def apply(ctx: SqlBaseParser.JoinRelationContext): UnresolvedJoin = {
    val primaryRelationName = ctx.parent.asInstanceOf[SqlBaseParser.RelationContext].relationPrimary.getText
    val secondaryRelationName = ctx.right.parent.asInstanceOf[SqlBaseParser.JoinRelationContext].relationPrimary.getText
    new UnresolvedJoin(primaryRelationName.replace("`",""), secondaryRelationName.replace("`",""))
  }
}

case class UnresolvedScan(tableName: String) extends LogicalPlan {
  override def describe(verbose: Boolean): String = "UnresolvedScan: " + "table=" + tableName
}

class UnresolvedSimpleJoin extends LogicalPlan {
  var left: UnresolvedScan = _
  var right: UnresolvedScan = _

  override def describe(verbose: Boolean): String = "UnresolvedSimpleJoin"
}

object UnresolvedSimpleJoin {
  def apply(left: UnresolvedScan, right: UnresolvedScan): UnresolvedSimpleJoin = {
    val j = new UnresolvedSimpleJoin
    j.left = left
    j.right = right
    j.addChildren(left)
    j.addChildren(right)
    j
  }
}

/** Unresolved where clause */
case class UnresolvedWhere(var tableName: String, var filedName: String, var operator: String) extends LogicalPlan {
  override def describe(verbose: Boolean): String = "UnresolvedWhere: " + "table=" + tableName + " filed=" + filedName + " operator=" + operator
}

