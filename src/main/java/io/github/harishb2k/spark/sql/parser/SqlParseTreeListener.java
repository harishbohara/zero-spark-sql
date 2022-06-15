package io.github.harishb2k.spark.sql.parser;

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser.*;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParserBaseListener;
import io.github.harishb2k.spark.sql.parser.NodeDefinition.*;
import lombok.Data;

@SuppressWarnings("rawtypes")
@Data
public class SqlParseTreeListener extends SqlBaseParserBaseListener {
    private Node root;

    @Override
    public void enterSingleStatement(SingleStatementContext ctx) {
        root = new NodeDefinition.SingleSelect(ctx);
    }

    @Override
    public void enterFromClause(FromClauseContext ctx) {
        FromClause n = new FromClause(ctx);
        root.addChildren(n);
        root = n;
    }

    @Override
    public void exitFromClause(FromClauseContext ctx) {
        root = root.getParent();
    }
}