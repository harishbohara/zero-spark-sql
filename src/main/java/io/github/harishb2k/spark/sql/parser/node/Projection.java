package io.github.harishb2k.spark.sql.parser.node;

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;

import java.util.ArrayList;
import java.util.List;

public class Projection extends Node<SqlBaseParser.SelectClauseContext> {
    private final List<String> columns;

    public Projection(SqlBaseParser.SelectClauseContext ctx) {
        super(ctx);
        columns = new ArrayList<>();
        for (SqlBaseParser.NamedExpressionContext n : ctx.namedExpressionSeq().namedExpression()) {
            columns.add(n.getText());
        }
    }

    @Override
    public String describe(boolean verbose) {
        return "Projection: " + String.join(",", columns);
    }
}
