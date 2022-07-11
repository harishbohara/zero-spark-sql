package io.github.harishb2k.spark.sql.parser.node;

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;

import java.util.ArrayList;
import java.util.List;

public class UnresolvedProjection extends Node<SqlBaseParser.SelectClauseContext> {
    private final List<String> columns;

    public UnresolvedProjection(SqlBaseParser.SelectClauseContext ctx) {
        super(ctx);
        columns = new ArrayList<>();
        for (SqlBaseParser.NamedExpressionContext n : ctx.namedExpressionSeq().namedExpression()) {
            columns.add(n.getText());
        }
    }

    @Override
    public String describe(boolean verbose) {
        return "UnresolvedProjection: " + String.join(",", columns);
    }
}
