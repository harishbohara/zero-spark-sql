package io.github.harishb2k.spark.sql.parser.node;

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParserBaseListener;
import lombok.Getter;
import org.antlr.v4.runtime.ParserRuleContext;

public class SqlParseTreeListener extends SqlBaseParserBaseListener {
    @Getter
    private Node<? extends ParserRuleContext> parentNode;
    private Node<? extends ParserRuleContext> internalRootNode;

    @Override
    public void enterSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        SingleSelect t = new SingleSelect(ctx);
        parentNode = internalRootNode = t;
    }

    @Override
    public void exitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        commonExit();
    }

    @Override
    public void enterSelectClause(SqlBaseParser.SelectClauseContext ctx) {
        Projection t = new Projection(ctx);
        commonAddChildren(t);
    }

    @Override
    public void exitSelectClause(SqlBaseParser.SelectClauseContext ctx) {
        commonExit();
    }

    private void commonAddChildren(Node<? extends ParserRuleContext> node) {
        internalRootNode.addChildren(node);
        internalRootNode = node;
    }

    private void commonExit() {
        internalRootNode = internalRootNode.getParent();
    }
}
