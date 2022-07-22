package io.github.harishb2k.spark.sql.parser.logical.optimizer;


import io.github.harishb2k.spark.sql.parser.node.Node;

import java.util.ArrayList;
import java.util.List;

public class LogicalPlanOptimizer {
    private final List<LogicalRule> rules;

    public LogicalPlanOptimizer() {
        this.rules = new ArrayList<>();
        this.rules.add(new WhereClauseCollapseRuleExt());
    }

    public Object optimize(Node node) {
        for (LogicalRule rule : rules) {
            rule.transform(node);
        }
        for (Node n : node.getChildren()) {
            optimize(n);
        }
        return node;
    }
}
