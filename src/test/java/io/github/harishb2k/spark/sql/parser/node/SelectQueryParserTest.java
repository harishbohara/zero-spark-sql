package io.github.harishb2k.spark.sql.parser.node;

import io.github.harishb2k.spark.grammar.parser.SqlBaseLexer;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Test;

public class SelectQueryParserTest {

    @Test
    public void parseSelectQuery() {
        String statement = """ 
                 SELECT `id`, `name`, `created_at`
                 FROM `main_table`
                 WHERE `main_table`.`id` > 10;
                """;

        var lexer = new SqlBaseLexer(CharStreams.fromString(statement));
        var tokens = new CommonTokenStream(lexer);
        var parser = new SqlBaseParser(tokens);
        ParseTreeWalker walker = new ParseTreeWalker();
        var listener = new io.github.harishb2k.spark.sql.parser.node.SqlParseTreeListenerExt();
        walker.walk(listener, parser.singleStatement());

        var root = listener.parentNode();
        root.print(1);
    }

    @Test
    public void parseSelectQueryWithJoin() {
        String statement = """
                 SELECT `id`, `name`, `created_at`
                 FROM `main_table` INNER JOIN `other_table`
                    ON `main_table`.`column_name_main` = `other_table`.`column_name_other`
                 WHERE `main_table`.`id` > 10;
                """;

      /* statement = """
                 SELECT `id`, `name`, `created_at`
                 FROM `main_table`
                 WHERE `main_table`.`id` > 10;
                """;*/

        var lexer = new SqlBaseLexer(CharStreams.fromString(statement));
        var tokens = new CommonTokenStream(lexer);
        var parser = new SqlBaseParser(tokens);
        ParseTreeWalker walker = new ParseTreeWalker();
        var listener = new io.github.harishb2k.spark.sql.parser.node.SqlParseTreeListenerExt();
        walker.walk(listener, parser.singleStatement());

        var root = listener.parentNode();
        root.print(1);
    }
}
