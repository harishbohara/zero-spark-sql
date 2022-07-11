package io.github.harishb2k.spark.sql.parser.node;

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParser.JoinRelationContext;

public class UnresolvedJoin extends Node<JoinRelationContext> {
    private final SqlBaseParser.RelationPrimaryContext primaryRelationshipContext;
    private final String primaryRelationName;
    private final String secondaryRelationName;

    public UnresolvedJoin(JoinRelationContext ctx) {
        super(ctx);
        primaryRelationshipContext = ctx.relationPrimary();
        primaryRelationName = ((SqlBaseParser.RelationContext) ctx.parent).relationPrimary().getText();
        secondaryRelationName = ((JoinRelationContext) ctx.right.parent).relationPrimary().getText();
    }

    @Override
    public String describe(boolean verbose) {
        return "UnresolvedJoin: PrimaryRelation=" + primaryRelationName + " SecondaryRelation=" + secondaryRelationName;
    }
}
