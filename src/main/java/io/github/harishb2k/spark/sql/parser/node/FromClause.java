package io.github.harishb2k.spark.sql.parser.node;

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser.FromClauseContext;

public class FromClause extends Node<FromClauseContext> {
    public FromClause(FromClauseContext ctx) {
        super(ctx);
    }

    @Override
    public String describe(boolean verbose) {
        return "FromClause";
    }
}
