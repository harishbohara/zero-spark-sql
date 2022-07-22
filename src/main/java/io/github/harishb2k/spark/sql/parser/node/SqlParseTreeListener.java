package io.github.harishb2k.spark.sql.parser.node;

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParserBaseListener;
import lombok.Getter;

public class SqlParseTreeListener extends SqlBaseParserBaseListener {
    @Getter
    private Node parentNode;
    private Node internalRootNode;

    @Override
    public void enterSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        UnresolvedSingleSelect t = new UnresolvedSingleSelect(ctx);
        parentNode = internalRootNode = t;
    }

    @Override
    public void exitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        commonExit();
    }

    @Override
    public void enterSelectClause(SqlBaseParser.SelectClauseContext ctx) {
        UnresolvedProjection t = new UnresolvedProjection(ctx);
        commonAddChildren(t);
    }

    @Override
    public void exitSelectClause(SqlBaseParser.SelectClauseContext ctx) {
        commonExit();
    }

    @Override
    public void enterFromClause(SqlBaseParser.FromClauseContext ctx) {
        FromClause t = new FromClause(ctx);
        commonAddChildren(t);
    }

    @Override
    public void exitFromClause(SqlBaseParser.FromClauseContext ctx) {
        commonExit();
    }

    @Override
    public void enterJoinRelation(SqlBaseParser.JoinRelationContext ctx) {
        UnresolvedJoin t = new UnresolvedJoin(ctx);
        commonAddChildren(t);
    }

    @Override
    public void enterWhereClause(SqlBaseParser.WhereClauseContext ctx) {
        UnresolvedWhere t = new UnresolvedWhere(ctx);
        commonAddChildren(t);

    }

    @Override
    public void exitWhereClause(SqlBaseParser.WhereClauseContext ctx) {
        commonExit();
    }

    @Override
    public void enterComparison(SqlBaseParser.ComparisonContext ctx) {
        super.enterComparison(ctx);
        if (internalRootNode instanceof UnresolvedWhere w) {
            w.tableName = ((SqlBaseParser.DereferenceContext) ((SqlBaseParser.ValueExpressionDefaultContext) ctx.left).primaryExpression()).base.getText();
            w.filedName = ((SqlBaseParser.DereferenceContext) ((SqlBaseParser.ValueExpressionDefaultContext) ctx.left).primaryExpression()).fieldName.getText();
            w.operator = ctx.comparisonOperator().getText();
        }
    }

    @Override
    public void exitJoinRelation(SqlBaseParser.JoinRelationContext ctx) {
        commonExit();
    }

    private void commonAddChildren(Node node) {
        internalRootNode.addChildren(node);
        internalRootNode = node;
    }

    private void commonExit() {
        internalRootNode = internalRootNode.getParent();
    }
}
