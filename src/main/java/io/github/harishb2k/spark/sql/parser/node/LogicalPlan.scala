package io.github.harishb2k.spark.sql.parser.node

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser
import org.apache.commons.lang3.StringUtils

import java.util
import scala.collection.JavaConversions._

abstract class LogicalPlan {

  /**
   * Parent of this plan
   */
  var parent: LogicalPlan = _;


  protected val internalChildren = new util.ArrayList[LogicalPlan]

  /**
   * Add a children to this node
   */
  def addChildren(logicalPlan: LogicalPlan): Unit = {
    internalChildren.add(logicalPlan)
    logicalPlan.parent = this
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

/** Unresolved where clause */
class UnresolvedWhere(var tableName: String, var filedName: String, var operator: String) extends LogicalPlan {
  override def describe(verbose: Boolean): String = "UnresolvedWhere: " + "table=" + tableName + " filed=" + filedName + " operator=" + operator
}

