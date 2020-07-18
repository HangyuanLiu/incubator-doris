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
package org.apache.doris.sql.planner.plan;

import com.google.common.collect.Maps;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.tree.Expression;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;

public class AssignmentUtils
{
    private AssignmentUtils() {}

    @Deprecated
    public static Map.Entry<VariableReferenceExpression, RowExpression> identityAsSymbolReference(VariableReferenceExpression variable)
    {
        return singletonMap(variable, castToRowExpression(asSymbolReference(variable)))
                .entrySet().iterator().next();
    }

    @Deprecated
    public static Map<VariableReferenceExpression, RowExpression> identitiesAsSymbolReferences(Collection<VariableReferenceExpression> variables)
    {
        Map<VariableReferenceExpression, RowExpression> map = new LinkedHashMap<>();
        for (VariableReferenceExpression variable : variables) {
            map.put(variable, castToRowExpression(asSymbolReference(variable)));
        }
        return map;
    }

    @Deprecated
    public static Assignments identityAssignmentsAsSymbolReferences(Collection<VariableReferenceExpression> variables)
    {
        return Assignments.builder().putAll(identitiesAsSymbolReferences(variables)).build();
    }

    public static Assignments identityAssignments(Collection<VariableReferenceExpression> variables)
    {
        Assignments.Builder builder = Assignments.builder();
        variables.forEach(variable -> builder.put(variable, variable));
        return builder.build();
    }

    public static Assignments identityAssignments(VariableReferenceExpression... variables)
    {
        return identityAssignments(asList(variables));
    }

    public static boolean isIdentity(Assignments assignments, VariableReferenceExpression output)
    {
        RowExpression value = assignments.get(output);
        return value instanceof VariableReferenceExpression && ((VariableReferenceExpression) value).getName().equals(output.getName());
    }

    public static Assignments rewrite(Assignments assignments, Function<Expression, Expression> rewrite)
    {
        return assignments.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), castToRowExpression(rewrite.apply(castToExpression(entry.getValue())))))
                .collect(toAssignments());
    }

    private static Collector<Map.Entry<VariableReferenceExpression, RowExpression>, Assignments.Builder, Assignments> toAssignments()
    {
        return Collector.of(
                Assignments::builder,
                (builder, entry) -> builder.put(entry.getKey(), entry.getValue()),
                (left, right) -> {
                    left.putAll(right.build());
                    return left;
                },
                Assignments.Builder::build);
    }
}