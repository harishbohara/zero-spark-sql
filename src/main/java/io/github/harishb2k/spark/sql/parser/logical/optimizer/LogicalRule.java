package io.github.harishb2k.spark.sql.parser.logical.optimizer;

import io.github.harishb2k.spark.sql.parser.node.Node;

public interface LogicalRule {

    Node transform(Node node);
}
