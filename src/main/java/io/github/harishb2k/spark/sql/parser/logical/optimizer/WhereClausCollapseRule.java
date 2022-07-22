package io.github.harishb2k.spark.sql.parser.logical.optimizer;

import io.github.harishb2k.spark.sql.parser.node.Node;
import io.github.harishb2k.spark.sql.parser.node.UnresolvedSingleSelect;
import io.github.harishb2k.spark.sql.parser.node.UnresolvedWhere;

public class WhereClausCollapseRule implements LogicalRule {
    @Override
    public Node transform(Node node) {
        if (node instanceof UnresolvedWhere w) {
            if (node.getParent() instanceof UnresolvedSingleSelect p) {

            }
        }
        return null;
    }
}

