package io.github.harishb2k.spark.sql.parser.logical.optimizer;

import io.github.harishb2k.spark.grammar.parser.SqlBaseLexer;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;
import io.github.harishb2k.spark.sql.parser.node.LogicalPlan;
import io.github.harishb2k.spark.sql.parser.node.SqlParseTreeListenerExt;
import io.github.harishb2k.spark.sql.parser.node.UnresolvedJoin;
import io.github.harishb2k.spark.sql.parser.node.UnresolvedScan;
import io.github.harishb2k.spark.sql.parser.node.UnresolvedSimpleJoin;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Assert;
import org.junit.Test;

public class SelectQueryOptimizerTest {

    @Test
    public void parseSelectQuery_UnresolvedJoinRule() {
        String statement = """ 
                 SELECT `id`, `name`, `created_at`
                 FROM `main_table`
                 WHERE `main_table`.`id` > 10;
                """;

        var lexer = new SqlBaseLexer(CharStreams.fromString(statement));
        var tokens = new CommonTokenStream(lexer);
        var parser = new SqlBaseParser(tokens);
        ParseTreeWalker walker = new ParseTreeWalker();
        var listener = new SqlParseTreeListenerExt();
        walker.walk(listener, parser.singleStatement());

        var root = listener.parentNode();
        root.print(1);

        // Test by running only UnresolvedJoinRule
        System.out.println("\nAfter Optimize called \n");
        LogicalPlanOptimizer optimizer = new LogicalPlanOptimizer();
        optimizer.rules().clear();
        optimizer.rules().add(new UnresolvedJoinRule());
        root = optimizer.optimize(root);
        root.print(1);
    }

    @Test
    public void parseSelectQueryWithJoin_UnresolvedJoinRule() {
        String statement = """
                 SELECT `id`, `name`, `created_at`
                 FROM `main_table` INNER JOIN `other_table`
                    ON `main_table`.`column_name_main` = `other_table`.`column_name_other`
                 WHERE `main_table`.`id` > 10;
                """;

        var lexer = new SqlBaseLexer(CharStreams.fromString(statement));
        var tokens = new CommonTokenStream(lexer);
        var parser = new SqlBaseParser(tokens);
        ParseTreeWalker walker = new ParseTreeWalker();
        var listener = new SqlParseTreeListenerExt();
        walker.walk(listener, parser.singleStatement());

        var root = listener.parentNode();
        root.print(1);

        // Test by running only UnresolvedJoinRule
        System.out.println("\nAfter Optimize called \n");
        LogicalPlanOptimizer optimizer = new LogicalPlanOptimizer();
        optimizer.rules().clear();
        optimizer.rules().add(new UnresolvedJoinRule());
        root = optimizer.optimize(root);
        root.print(1);

        // Test 1 - we must not have any UnresolvedJoin
        LogicalPlan result = root.travers(plan -> plan instanceof UnresolvedJoin ? plan : null);
        Assert.assertNull(result);

        // Test - we must have UnresolvedSimpleJoin which is replaced by UnresolvedJoin
        UnresolvedSimpleJoin resultUnresolvedSimpleJoin = (UnresolvedSimpleJoin) root.travers(plan -> plan instanceof UnresolvedSimpleJoin ? plan : null);
        Assert.assertNotNull(resultUnresolvedSimpleJoin);
        Assert.assertEquals(true, resultUnresolvedSimpleJoin.left() instanceof UnresolvedScan);
        Assert.assertEquals(true, resultUnresolvedSimpleJoin.right() instanceof UnresolvedScan);
        Assert.assertEquals("main_table", ((UnresolvedScan) resultUnresolvedSimpleJoin.left()).tableName());
        Assert.assertEquals("other_table", ((UnresolvedScan) resultUnresolvedSimpleJoin.right()).tableName());

    }
}
