/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.doris.sql.tree;

public abstract class DefaultTraversalVisitor<R, C>
        extends AstVisitor<R, C>
{
    @Override
    protected R visitArithmeticBinary(ArithmeticBinaryExpression node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitComparisonExpression(ComparisonExpression node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitQuery(Query node, C context)
    {
        if (node.getWith().isPresent()) {
            process(node.getWith().get(), context);
        }
        process(node.getQueryBody(), context);
        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }

        return null;
    }

    @Override
    protected R visitWith(With node, C context)
    {
        for (WithQuery query : node.getQueries()) {
            process(query, context);
        }

        return null;
    }

    @Override
    protected R visitWithQuery(WithQuery node, C context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected R visitSelect(Select node, C context)
    {
        for (SelectItem item : node.getSelectItems()) {
            process(item, context);
        }

        return null;
    }

    @Override
    protected R visitSingleColumn(SingleColumn node, C context)
    {
        process(node.getExpression(), context);

        return null;
    }

    @Override
    protected R visitFunctionCall(FunctionCall node, C context)
    {
        for (Expression argument : node.getArguments()) {
            process(argument, context);
        }

        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }

        if (node.getWindow().isPresent()) {
            process(node.getWindow().get(), context);
        }

        if (node.getFilter().isPresent()) {
            process(node.getFilter().get(), context);
        }

        return null;
    }

    @Override
    protected R visitArithmeticUnary(ArithmeticUnaryExpression node, C context)
    {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitOrderBy(OrderBy node, C context)
    {
        for (SortItem sortItem : node.getSortItems()) {
            process(sortItem, context);
        }
        return null;
    }

    @Override
    protected R visitSortItem(SortItem node, C context)
    {
        return process(node.getSortKey(), context);
    }

    @Override
    protected R visitQuerySpecification(QuerySpecification node, C context)
    {
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
        return null;
    }

    @Override
    protected R visitAliasedRelation(AliasedRelation node, C context)
    {
        return process(node.getRelation(), context);
    }

    @Override
    protected R visitSampledRelation(SampledRelation node, C context)
    {
        process(node.getRelation(), context);
        process(node.getSamplePercentage(), context);
        return null;
    }

    @Override
    protected R visitJoin(Join node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        node.getCriteria()
                .filter(criteria -> criteria instanceof JoinOn)
                .map(criteria -> process(((JoinOn) criteria).getExpression(), context));

        return null;
    }

    @Override
    protected R visitGroupBy(GroupBy node, C context)
    {
        for (GroupingElement groupingElement : node.getGroupingElements()) {
            process(groupingElement, context);
        }

        return null;
    }

    @Override
    protected R visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, C context)
    {
        process(node.getValue(), context);
        process(node.getSubquery(), context);

        return null;
    }
}
