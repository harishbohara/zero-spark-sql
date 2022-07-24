package io.github.harishb2k.spark.sql.parser.node

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser.{SingleStatementContext, TableNameContext}
import io.github.harishb2k.spark.grammar.parser.{SqlBaseParser, SqlBaseParserBaseListener}


class SqlParseTreeListenerExt extends SqlBaseParserBaseListener {

  var parentNode: LogicalPlan = _
  private var internalRootNode: LogicalPlan = _
  private val parserCtx: Ctx = new Ctx()


  // Called when we find a statement
  override def enterSingleStatement(ctx: SingleStatementContext): Unit = {
    val t = new UnresolvedSingleSelect
    parentNode = t
    internalRootNode = t
    parserCtx.query = t
  }

  // Called when we exit from a statement
  override def exitSingleStatement(ctx: SqlBaseParser.SingleStatementContext): Unit = commonExit()

  // Called when we get a select statement
  override def enterSelectClause(ctx: SqlBaseParser.SelectClauseContext): Unit = {
    val t = UnresolvedProjection(ctx)
    parserCtx.query.projection = t
    commonAddChildren(t)
  }

  // Called when we exit from a select statement
  override def exitSelectClause(ctx: SqlBaseParser.SelectClauseContext): Unit = commonExit()

  // Called when we get a from clause
  override def enterFromClause(ctx: SqlBaseParser.FromClauseContext): Unit = {
    val c = ctx.relation(0).relationPrimary().asInstanceOf[TableNameContext]
    val t = new UnresolvedFromClause(c.getText)
    parserCtx.query.from = t
    commonAddChildren(t)
  }

  // Called when we exit from a from clause
  override def exitFromClause(ctx: SqlBaseParser.FromClauseContext): Unit = commonExit()

  // Called when we get a join clause
  override def enterJoinRelation(ctx: SqlBaseParser.JoinRelationContext): Unit = {
    val t = UnresolvedJoin(ctx)
    commonAddChildren(t)
  }

  // Called when we exit a join clause
  override def exitJoinRelation(ctx: SqlBaseParser.JoinRelationContext): Unit = commonExit()


  // Called when we get a where clause
  override def enterWhereClause(ctx: SqlBaseParser.WhereClauseContext): Unit = {
    val t = new UnresolvedWhere(null, null, null)
    parserCtx.query.where = t
    commonAddChildren(t)
  }

  // Called when we exit a where clause
  override def exitWhereClause(ctx: SqlBaseParser.WhereClauseContext): Unit = {
    commonExit()
  }

  // Called when we get a compare expression
  override def enterComparison(ctx: SqlBaseParser.ComparisonContext): Unit = {
    internalRootNode match {
      case _: UnresolvedWhere =>
        parserCtx.query.where.tableName = (ctx.left.asInstanceOf[SqlBaseParser.ValueExpressionDefaultContext]).primaryExpression.asInstanceOf[SqlBaseParser.DereferenceContext].base.getText
        parserCtx.query.where.filedName = (ctx.left.asInstanceOf[SqlBaseParser.ValueExpressionDefaultContext]).primaryExpression.asInstanceOf[SqlBaseParser.DereferenceContext].fieldName.getText
        parserCtx.query.where.operator = ctx.comparisonOperator.getText
      case _ =>
    }
  }

  def commonExit(): Unit = {
    internalRootNode = internalRootNode.parent
  }

  private def commonAddChildren(logicalPlan: LogicalPlan): Unit = {
    internalRootNode.addChildren(logicalPlan)
    internalRootNode = logicalPlan
  }

  private class Ctx {
    var query: UnresolvedSingleSelect = _
  }
}
