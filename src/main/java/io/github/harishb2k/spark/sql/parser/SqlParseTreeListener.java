package io.github.harishb2k.spark.sql.parser;

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser.CreateOrReplaceTableColTypeListContext;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParser.CreateTableClausesContext;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParser.CreateTableHeaderContext;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParser.FromClauseContext;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParser.SingleStatementContext;
import io.github.harishb2k.spark.grammar.parser.SqlBaseParserBaseListener;
import io.github.harishb2k.spark.sql.parser.NodeDefinition.FromClause;
import io.github.harishb2k.spark.sql.parser.NodeDefinition.Node;
import io.github.harishb2k.spark.sql.parser.NodeDefinition.UnresolvedCreateTable;
import lombok.Data;

@SuppressWarnings("rawtypes")
@Data
public class SqlParseTreeListener extends SqlBaseParserBaseListener {
    private Node root;

    @Override
    public void enterSingleStatement(SingleStatementContext ctx) {
        root = new NodeDefinition.SingleSelect(ctx);
    }

    @Override
    public void enterFromClause(FromClauseContext ctx) {
        FromClause n = new FromClause(ctx);
        root.addChildren(n);
        root = n;
    }

    @Override
    public void exitFromClause(FromClauseContext ctx) {
        root = root.getParent();
    }


    // Everything related to table
    @Override
    public void enterCreateTableHeader(CreateTableHeaderContext ctx) {
        root = new UnresolvedCreateTable(ctx);
        UnresolvedCreateTable t = (UnresolvedCreateTable) root;
        t.setTableName(ctx.multipartIdentifier().parts.get(0).getText());
    }

    @Override
    public void enterCreateTableClauses(CreateTableClausesContext ctx) {

        // Extract table properties
        ctx.tableProps.property().forEach(propertyContext -> {
            if (root instanceof UnresolvedCreateTable t) {
                t.getTableProperties().put(propertyContext.propertyKey().STRING().getText(), propertyContext.propertyValue().STRING().getText());
            }
        });

        // Extract partition information
        ctx.partitionFieldList().forEach(partitionFieldContext -> {
            if (root instanceof UnresolvedCreateTable t) {
                t.getPartitionFields().add(partitionFieldContext.getPayload().getText());
            }
        });

        ctx.options.property().forEach(propertyContext -> {
            if (root instanceof UnresolvedCreateTable t) {
                t.getOptions().put(propertyContext.propertyKey().getText(), propertyContext.propertyValue().getText());
            }
        });
    }

    @Override
    public void enterCreateOrReplaceTableColTypeList(CreateOrReplaceTableColTypeListContext ctx) {
        ctx.createOrReplaceTableColType().forEach(colCtx -> {
            if (root instanceof UnresolvedCreateTable t) {
                t.getCols().put(colCtx.colName.getText(), colCtx.dataType().getText());
            }
        });
    }
}