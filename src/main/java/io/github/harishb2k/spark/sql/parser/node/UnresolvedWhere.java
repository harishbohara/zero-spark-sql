package io.github.harishb2k.spark.sql.parser.node;

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UnresolvedWhere extends Node<SqlBaseParser.WhereClauseContext> {
    public String tableName;
    public String filedName;
    public String operator;

    public UnresolvedWhere(SqlBaseParser.WhereClauseContext ctx) {
        super(ctx);
    }

    @Override
    public String describe(boolean verbose) {
        return "Where: " + "table=" + tableName + " filed=" + filedName + " operator=" + operator;
    }
}
