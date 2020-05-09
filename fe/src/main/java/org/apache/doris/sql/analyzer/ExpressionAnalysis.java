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
package org.apache.doris.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.sql.metadata.Type;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.NodeRef;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ExpressionAnalysis
{
    private final Map<NodeRef<Expression>, Type> expressionTypes;
    private final Map<NodeRef<Expression>, Type> expressionCoercions;
    private final Set<NodeRef<Expression>> typeOnlyCoercions;
    private final Map<NodeRef<Expression>, FieldId> columnReferences;

    public ExpressionAnalysis(
            Map<NodeRef<Expression>, Type> expressionTypes,
            Map<NodeRef<Expression>, Type> expressionCoercions,
            Map<NodeRef<Expression>, FieldId> columnReferences,
            Set<NodeRef<Expression>> typeOnlyCoercions)
    {
        this.expressionTypes = ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
        this.expressionCoercions = ImmutableMap.copyOf(requireNonNull(expressionCoercions, "expressionCoercions is null"));
        this.typeOnlyCoercions = ImmutableSet.copyOf(requireNonNull(typeOnlyCoercions, "typeOnlyCoercions is null"));
        this.columnReferences = ImmutableMap.copyOf(requireNonNull(columnReferences, "columnReferences is null"));
    }

    public Type getType(Expression expression)
    {
        return expressionTypes.get(NodeRef.of(expression));
    }

    public Map<NodeRef<Expression>, Type> getExpressionTypes()
    {
        return expressionTypes;
    }

    public Type getCoercion(Expression expression)
    {
        return expressionCoercions.get(NodeRef.of(expression));
    }

    public boolean isTypeOnlyCoercion(Expression expression)
    {
        return typeOnlyCoercions.contains(NodeRef.of(expression));
    }

    public boolean isColumnReference(Expression node)
    {
        return columnReferences.containsKey(NodeRef.of(node));
    }
}
