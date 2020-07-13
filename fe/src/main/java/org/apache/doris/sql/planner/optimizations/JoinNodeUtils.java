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
package org.apache.doris.sql.planner.optimizations;


import com.google.common.collect.ImmutableList;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relational.FunctionResolution;
import org.apache.doris.sql.tree.ComparisonExpression;
import org.apache.doris.sql.tree.Join;
import org.apache.doris.sql.tree.SymbolReference;
import org.apache.doris.sql.type.OperatorType;

import static org.apache.doris.sql.planner.plan.JoinNode.Type.*;
import static org.apache.doris.sql.relational.Expressions.call;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.EQUAL;
import static org.apache.doris.sql.type.BooleanType.BOOLEAN;

public final class JoinNodeUtils
{
    private JoinNodeUtils() {}

    public static ComparisonExpression toExpression(JoinNode.EquiJoinClause clause)
    {
        return new ComparisonExpression(EQUAL, new SymbolReference(clause.getLeft().getName()), new SymbolReference(clause.getRight().getName()));
    }

    public static RowExpression toRowExpression(JoinNode.EquiJoinClause clause, FunctionResolution functionResolution)
    {
        return call(
                OperatorType.EQUAL.name(),
                functionResolution.comparisonFunction(OperatorType.EQUAL, clause.getLeft().getType(), clause.getRight().getType()),
                BOOLEAN,
                ImmutableList.of(clause.getLeft(), clause.getRight()));
    }

    public static JoinNode.Type typeConvert(Join.Type joinType)
    {
        // Omit SEMI join types because they must be inferred by the planner and not part of the SQL parse tree
        switch (joinType) {
            case CROSS:
            case IMPLICIT:
            case INNER:
                return INNER;
            case LEFT:
                return LEFT;
            case RIGHT:
                return RIGHT;
            case FULL:
                return FULL;
            default:
                throw new UnsupportedOperationException("Unsupported join type: " + joinType);
        }
    }
}
