package io.github.harishb2k.spark.sql.parser.node;

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;

public class UnresolvedSingleSelect extends Node {
    public UnresolvedSingleSelect(SqlBaseParser.SingleStatementContext ctx) {
    }

    @Override
    public String describe(boolean verbose) {
        return "UnresolvedParentSelect";
    }
}
