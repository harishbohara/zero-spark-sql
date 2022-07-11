package org.example;

import io.github.harishb2k.spark.grammar.parser.SqlBaseLexer;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;
import io.github.harishb2k.spark.sql.parser.NodeDefinition.Node;
import io.github.harishb2k.spark.sql.parser.SqlParseTreeListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Test;

public class HiveCreateTableTestCase {

    @Test
    public void createHiveTable() {
        String statement = "-- CREATE a HIVE SerDe table using the CREATE TABLE USING syntax.\n" +
                "CREATE TABLE `my_table` (`name` STRING, `age` INT, `hair_color` STRING)\n" +
                "  USING HIVE\n" +
                "  OPTIONS(\n" +
                "      INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat',\n" +
                "      OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat',\n" +
                "      SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe')\n" +
                "  PARTITIONED BY (`hair_color`)\n" +
                "  TBLPROPERTIES ('status'='staging', 'owner'='andrew');";

        SqlBaseLexer lexer = new SqlBaseLexer(CharStreams.fromString(statement));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SqlBaseParser parser = new SqlBaseParser(tokens);


        ParseTreeWalker walker = new ParseTreeWalker();
        SqlParseTreeListener listener = new SqlParseTreeListener();
        walker.walk(listener, parser.singleStatement());

        System.out.println(listener.getRoot());
        listener.getRoot().print(1);
        Node root = listener.getRoot();
        System.out.println();

        System.out.println(listener.getRoot().graph());
    }

    @Test
    public void selectTest() {
        String statement = """
                 SELECT `id`, `name`, `created_at`
                 FROM `main_table` INNER JOIN `other_table`
                    ON `main_table`.`column_name_main` = `other_table`.`column_name_other`
                 WHERE `id` > 10;
                """;

        var lexer = new SqlBaseLexer(CharStreams.fromString(statement));
        var tokens = new CommonTokenStream(lexer);
        var parser = new SqlBaseParser(tokens);
        ParseTreeWalker walker = new ParseTreeWalker();
        var listener = new io.github.harishb2k.spark.sql.parser.node.SqlParseTreeListener();
        walker.walk(listener, parser.singleStatement());


        var root = listener.getParentNode();
        root.print(1);

        // System.out.println(listener.getRoot().graph());
    }
}
