package io.github.harishb2k.spark.sql.parser.node;

public interface ITTraversal {
    LogicalPlan callback(LogicalPlan plan);
}
