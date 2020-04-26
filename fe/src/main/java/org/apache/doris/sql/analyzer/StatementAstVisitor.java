package org.apache.doris.sql.analyzer;

import org.apache.doris.analysis.ParseNode;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.QueryBody;
import org.apache.doris.sql.tree.QuerySpecification;

public abstract class StatementAstVisitor<R, C> {
    /*
    public R process(StatementBase stmt)
    {
        return process(stmt, null);
    }

     */

    public R process(StatementBase stmt, Node node, C context) {
        return stmt.accept(this, node, context);
    }

    public R visitNode(StatementBase stmt, Node node, C context) {
        return null;
    }

    public R visitStatement(StatementBase node, C context)
    {
        //return visitNode(node, context);
        return null;
    }

    public R visitQuery(QueryStmt node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitQueryBody(QueryBody node, C context)
    {
        //return visitRelation(node, context);
        return null;
    }

    public R visitQuerySpecification(SelectStmt stmt, QuerySpecification node, C context) {
        //return visitQueryBody(node, context);
        return null;
    }
}
