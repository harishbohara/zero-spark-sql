package io.github.harishb2k.spark.sql.parser.node;

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;

public class SingleSelect extends Node<SqlBaseParser.SingleStatementContext> {
    public SingleSelect(SqlBaseParser.SingleStatementContext ctx) {
        super(ctx);
    }

    @Override
    public String describe(boolean verbose) {
        return "ParentSelect";
    }
}
