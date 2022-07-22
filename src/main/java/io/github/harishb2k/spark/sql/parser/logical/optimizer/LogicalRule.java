package io.github.harishb2k.spark.sql.parser.logical.optimizer;

import io.github.harishb2k.spark.sql.parser.node.LogicalPlan;

public interface LogicalRule {
    LogicalPlan transform(LogicalPlan plan);
}
