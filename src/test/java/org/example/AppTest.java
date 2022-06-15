package org.example;

import static org.junit.Assert.assertTrue;

import io.github.harishb2k.spark.grammar.parser.SqlBaseLexer;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;
import io.github.harishb2k.spark.sql.parser.SqlParseTreeListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Test;

public class AppTest {

    @Test
    public void shouldAnswerWithTrue() {
        assertTrue(true);
        String javaClassContent = "SELECT `id` FROM `users` INNER JOIN `table1` ON `table1`.`column_name` = `table2`.`column_name` WHERE `id` > 10;";
        SqlBaseLexer lexer = new SqlBaseLexer(CharStreams.fromString(javaClassContent));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SqlBaseParser parser = new SqlBaseParser(tokens);


        ParseTreeWalker walker = new ParseTreeWalker();
        SqlParseTreeListener listener = new SqlParseTreeListener();
        walker.walk(listener, parser.singleStatement());

        System.out.println(listener.getRoot());
        listener.getRoot().print(1);
    }
}
