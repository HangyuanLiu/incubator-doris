package org.apache.doris.sql.analyzer;

import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.sql.tree.QuerySpecification;

import java.util.Optional;

public abstract class DefaultTraversalVisitor<R, C>
        extends StatementAstVisitor<R, C> {


    @Override
    public R visitQuery(QueryStmt node, C context)
    {
        return null;
    }

    @Override
    public R visitQuerySpecification(SelectStmt stmt, QuerySpecification node, C context)
    {
        /*
        process(node.getSelect(), context);
        if (node.getFrom().isPresent()) {
            process(node.getFrom().get(), context);
        }
        if (node.getWhere().isPresent()) {
            process(node.getWhere().get(), context);
        }
        if (node.getGroupBy().isPresent()) {
            process(node.getGroupBy().get(), context);
        }
        if (node.getHaving().isPresent()) {
            process(node.getHaving().get(), context);
        }
        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }
         */
        return null;
    }
}
