package io.github.harishb2k.spark.sql.parser;

import io.github.harishb2k.spark.grammar.parser.SqlBaseParser;
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
    private Node mainRoot;
    private Node root;

    @Override
    public void enterSingleStatement(SingleStatementContext ctx) {
        root = new NodeDefinition.SingleSelect(ctx);
        mainRoot = root;
    }

    @Override
    public void exitSingleStatement(SingleStatementContext ctx) {
        root = root.getParent();
    }

    @Override
    public void enterSelectClause(SqlBaseParser.SelectClauseContext ctx) {
        super.enterSelectClause(ctx);
        NodeDefinition.SelectProjection t = new NodeDefinition.SelectProjection(ctx);
        root.addChildren(t);
        root = t;
    }

    @Override
    public void exitSelectClause(SqlBaseParser.SelectClauseContext ctx) {
        root = root.getParent();
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
        UnresolvedCreateTable n = new UnresolvedCreateTable(ctx);
        n.setTableName(ctx.multipartIdentifier().parts.get(0).getText());
        root.addChildren(n);
        root = n;
    }

    @Override
    public void exitCreateTableHeader(CreateTableHeaderContext ctx) {
        super.exitCreateTableHeader(ctx);
        root = root.getParent();
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

    @Override
    public void enterJoinRelation(SqlBaseParser.JoinRelationContext ctx) {
        NodeDefinition.UnresolvedJoin unresolvedJoin = new NodeDefinition.UnresolvedJoin(ctx);
        NodeDefinition.UnresolvedJoinRelation joinRelation = new NodeDefinition.UnresolvedJoinRelation(ctx);
        unresolvedJoin.addChildren(joinRelation);

        if (root instanceof NodeDefinition.UnresolvedRelation) {
            unresolvedJoin.addChildren(root);
            root = root.getParent();
            System.out.println("");
        }

        root.addChildren(unresolvedJoin);
        root = joinRelation;
    }

    @Override
    public void exitJoinRelation(SqlBaseParser.JoinRelationContext ctx) {
        root = root.getParent();
    }

    @Override
    public void enterRelation(SqlBaseParser.RelationContext ctx) {
        NodeDefinition.UnresolvedRelation t = new NodeDefinition.UnresolvedRelation(ctx);
        root.addChildren(t);
        root = t;
    }

    @Override
    public void exitRelation(SqlBaseParser.RelationContext ctx) {
        root = root.getParent();
    }
}