package io.github.harishb2k.spark.sql.parser;

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
            return "Current: " + getClass() + "->" + ctx.getText();
        }

        public void print(int depth) {
            char t = '\t';
            StringBuffer sb = new StringBuffer();
            for (Node n : children) {
                System.out.println(StringUtils.repeat(t, depth) + n);
                n.print(depth + 1);
            }
        }
    }

    public static class SingleSelect extends Node<SingleStatementContext> {
        public SingleSelect(SingleStatementContext ctx) {
            super(ctx);
        }
    }

    public static class FromClause extends Node<FromClauseContext> {
        public FromClause(FromClauseContext ctx) {
            super(ctx);
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
}
