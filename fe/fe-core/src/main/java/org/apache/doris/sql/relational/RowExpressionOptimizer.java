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
package org.apache.doris.sql.relational;

import org.apache.doris.sql.metadata.ConnectorSession;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.planner.RowExpressionInterpreter;
import org.apache.doris.sql.relation.ExpressionOptimizer;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.LiteralEncoder.toRowExpression;
import static org.apache.doris.sql.relation.ExpressionOptimizer.Level.OPTIMIZED;

public final class RowExpressionOptimizer
        implements ExpressionOptimizer
{
    private final Metadata metadata;

    public RowExpressionOptimizer(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public RowExpression optimize(RowExpression rowExpression, ExpressionOptimizer.Level level, ConnectorSession session)
    {
        if (level.ordinal() <= OPTIMIZED.ordinal()) {
            //return toRowExpression(new RowExpressionInterpreter(rowExpression, metadata, session, level).optimize(), rowExpression.getType());
            return rowExpression;
        }
        throw new IllegalArgumentException("Not supported optimization level: " + level);
    }

    @Override
    public Object optimize(RowExpression expression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
    {
        //RowExpressionInterpreter interpreter = new RowExpressionInterpreter(expression, metadata, session, level);
        //return interpreter.optimize(variableResolver::apply);
        return null;
    }
}
