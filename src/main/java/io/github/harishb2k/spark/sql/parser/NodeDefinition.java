package io.github.harishb2k.spark.sql.parser;

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParser.CreateTableHeaderContext;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParser.FromClauseContext;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParser.SingleStatementContext;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeDefinition {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Data
    @NoArgsConstructor
    public static class Node<T extends ParserRuleContext> {
        private T ctx;
        private Node parent;
        private final List<Node<ParserRuleContext>> children = new ArrayList<>();

        public Node(T ctx) {
            this.ctx = ctx;
        }

        public void addChildren(Node node) {
            children.add(node);
            node.setParent(this);
        }

        @Override
        public String toString() {
            String s = ctx.start.getText();
            // return "Current: " + getClass() + "->" + ctx.getText();
            return s;
        }

        public void print(int depth) {
            char t = '\t';


            String s = ctx.getText();
            if (str() != null) {
                s = str();
            }
            if (depth == 1) {
                System.out.println(s);
            }

            for (Node n : children) {
                s = n.ctx.start.getText();
                if (n.str() != null) {
                    s = n.str();
                }
                System.out.println(StringUtils.repeat(t, depth) + s);
                n.print(depth + 1);
            }
        }

        public String graph() {
            StringBuilder sb = new StringBuilder();
            for (Node n : children) {
                sb.append(ctx.getText() + " -> " + n.graph());
            }
            return sb.toString();
        }

        public String str() {
            return null;
        }
    }

    public static class SingleSelect extends Node<SingleStatementContext> {
        public SingleSelect(SingleStatementContext ctx) {
            super(ctx);
        }

        @Override
        public String str() {
            return "SELECT";
        }
    }

    public static class FromClause extends Node<FromClauseContext> {
        public FromClause(FromClauseContext ctx) {
            super(ctx);
        }

        @Override
        public String str() {
            return "FROM";
        }
    }

    @Data
    public static class UnresolvedCreateTable extends Node<CreateTableHeaderContext> {
        private String tableName;
        private List<String> partitionFields = new ArrayList<>();
        private Map<String, String> tableProperties = new HashMap<>();
        private Map<String, String> options = new HashMap<>();
        private Map<String, String> cols = new HashMap<>();

        public UnresolvedCreateTable(CreateTableHeaderContext ctx) {
            super(ctx);
        }
    }

    public static class UnresolvedProjection extends Node {

    }

    public static class UnresolvedRelation extends Node<SqlBaseParser.RelationContext> {
        private SqlBaseParser.RelationPrimaryContext relationPrimaryContext;
        private String tableName;

        public UnresolvedRelation(SqlBaseParser.RelationContext ctx) {
            super(ctx);
            relationPrimaryContext = ctx.relationPrimary();
            tableName = relationPrimaryContext.getText();
        }


        @Override
        public String str() {
            return "UnresolvedRelation: table=" + tableName;
        }
    }

    public static class UnresolvedJoinRelation extends Node<SqlBaseParser.JoinRelationContext> {
        private final SqlBaseParser.RelationPrimaryContext relationPrimaryContext;
        private final String tableName;

        public UnresolvedJoinRelation(SqlBaseParser.JoinRelationContext ctx) {
            super(ctx);
            relationPrimaryContext = ctx.relationPrimary();
            tableName = relationPrimaryContext.getText();
        }


        @Override
        public String str() {
            return "UnresolvedJoinRelation: table=" + tableName;
        }
    }

    public static class UnresolvedJoin extends Node<SqlBaseParser.JoinRelationContext> {
        public UnresolvedJoin(SqlBaseParser.JoinRelationContext ctx) {
            super(ctx);
        }

        @Override
        public String str() {
            return "Join";
        }
    }


    public static class SelectProjection extends Node<SqlBaseParser.SelectClauseContext> {
        private final List<String> columns;

        public SelectProjection(SqlBaseParser.SelectClauseContext ctx) {
            super(ctx);
            columns = new ArrayList<>();
            for (SqlBaseParser.NamedExpressionContext n : ctx.namedExpressionSeq().namedExpression()) {
                columns.add(n.getText());
            }
        }

        @Override
        public String str() {
            return "SelectProjection: " + String.join(",", columns);
        }

        @Override
        public String toString() {
            return "SELECT_PROJECTION";
        }
    }

}
