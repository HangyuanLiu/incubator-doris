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
package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.expressions.DefaultRowExpressionTraversalVisitor;
import org.apache.doris.sql.planner.iterative.Lookup;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.tree.DefaultTraversalVisitor;
import org.apache.doris.sql.tree.DereferenceExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.Identifier;
import org.apache.doris.sql.tree.NodeRef;
import org.apache.doris.sql.tree.QualifiedName;
import org.apache.doris.sql.tree.SymbolReference;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.ExpressionExtractor.extractExpressions;
import static org.apache.doris.sql.planner.ExpressionExtractor.extractExpressionsNonRecursive;
import static org.apache.doris.sql.planner.iterative.Lookup.noLookup;
import static org.apache.doris.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.isExpression;

public final class VariablesExtractor
{
    private VariablesExtractor() {}

    public static Set<VariableReferenceExpression> extractUnique(LogicalPlanNode node, TypeProvider types)
    {
        ImmutableSet.Builder<VariableReferenceExpression> unique = ImmutableSet.builder();
        extractExpressions(node).forEach(expression -> unique.addAll(extractUniqueVariableInternal(expression, types)));

        return unique.build();
    }

    public static Set<VariableReferenceExpression> extractUniqueNonRecursive(LogicalPlanNode node, TypeProvider types)
    {
        ImmutableSet.Builder<VariableReferenceExpression> uniqueVariables = ImmutableSet.builder();
        extractExpressionsNonRecursive(node).forEach(expression -> uniqueVariables.addAll(extractUniqueVariableInternal(expression, types)));

        return uniqueVariables.build();
    }

    public static Set<VariableReferenceExpression> extractUnique(LogicalPlanNode node, Lookup lookup, TypeProvider types)
    {
        ImmutableSet.Builder<VariableReferenceExpression> unique = ImmutableSet.builder();
        extractExpressions(node, lookup).forEach(expression -> unique.addAll(extractUniqueVariableInternal(expression, types)));
        return unique.build();
    }

    public static Set<VariableReferenceExpression> extractUnique(Expression expression, TypeProvider types)
    {
        return ImmutableSet.copyOf(extractAll(expression, types));
    }

    public static Set<VariableReferenceExpression> extractUnique(RowExpression expression)
    {
        return ImmutableSet.copyOf(extractAll(expression));
    }

    public static Set<VariableReferenceExpression> extractUnique(Iterable<? extends RowExpression> expressions)
    {
        ImmutableSet.Builder<VariableReferenceExpression> unique = ImmutableSet.builder();
        for (RowExpression expression : expressions) {
            unique.addAll(extractAll(expression));
        }
        return unique.build();
    }

    @Deprecated
    public static Set<VariableReferenceExpression> extractUnique(Iterable<? extends Expression> expressions, TypeProvider types)
    {
        ImmutableSet.Builder<VariableReferenceExpression> unique = ImmutableSet.builder();
        for (Expression expression : expressions) {
            unique.addAll(extractAll(expression, types));
        }
        return unique.build();
    }

    public static List<Symbol> extractAllSymbols(Expression expression)
    {
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        new SymbolBuilderVisitor().process(expression, builder);
        return builder.build();
    }

    public static List<VariableReferenceExpression> extractAll(Expression expression, TypeProvider types)
    {
        ImmutableList.Builder<VariableReferenceExpression> builder = ImmutableList.builder();
        new VariableFromExpressionBuilderVisitor(types).process(expression, builder);
        return builder.build();
    }

    public static List<VariableReferenceExpression> extractAll(RowExpression expression)
    {
        ImmutableList.Builder<VariableReferenceExpression> builder = ImmutableList.builder();
        expression.accept(new VariableBuilderVisitor(), builder);
        return builder.build();
    }

    // to extract qualified name with prefix
    public static Set<QualifiedName> extractNames(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
        new QualifiedNameBuilderVisitor(columnReferences).process(expression, builder);
        return builder.build();
    }

    public static Set<VariableReferenceExpression> extractOutputVariables(LogicalPlanNode planNode, Lookup lookup)
    {
        return searchFrom(planNode, lookup)
                .findAll()
                .stream()
                .flatMap(node -> node.getOutputVariables().stream())
                .collect(toImmutableSet());
    }

    private static Set<VariableReferenceExpression> extractUniqueVariableInternal(RowExpression expression, TypeProvider types)
    {
        if (isExpression(expression)) {
            return extractUnique(castToExpression(expression), types);
        }
        return extractUnique(expression);
    }

    private static class SymbolBuilderVisitor
            extends DefaultTraversalVisitor<Void, ImmutableList.Builder<Symbol>>
    {
        @Override
        protected Void visitSymbolReference(SymbolReference node, ImmutableList.Builder<Symbol> builder)
        {
            builder.add(Symbol.from(node));
            return null;
        }
    }

    private static class VariableFromExpressionBuilderVisitor
            extends DefaultTraversalVisitor<Void, ImmutableList.Builder<VariableReferenceExpression>>
    {
        private final TypeProvider types;

        protected VariableFromExpressionBuilderVisitor(TypeProvider types)
        {
            this.types = types;
        }

        @Override
        protected Void visitSymbolReference(SymbolReference node, ImmutableList.Builder<VariableReferenceExpression> builder)
        {
            builder.add(new VariableReferenceExpression(node.getName(), types.get(node)));
            return null;
        }
    }

    private static class VariableBuilderVisitor
            extends DefaultRowExpressionTraversalVisitor<ImmutableList.Builder<VariableReferenceExpression>>
    {
        @Override
        public Void visitVariableReference(VariableReferenceExpression variable, ImmutableList.Builder<VariableReferenceExpression> builder)
        {
            builder.add(variable);
            return null;
        }
    }

    private static class QualifiedNameBuilderVisitor
            extends DefaultTraversalVisitor<Void, ImmutableSet.Builder<QualifiedName>>
    {
        private final Set<NodeRef<Expression>> columnReferences;

        private QualifiedNameBuilderVisitor(Set<NodeRef<Expression>> columnReferences)
        {
            this.columnReferences = requireNonNull(columnReferences, "columnReferences is null");
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableSet.Builder<QualifiedName> builder)
        {
            if (columnReferences.contains(NodeRef.<Expression>of(node))) {
                builder.add(DereferenceExpression.getQualifiedName(node));
            }
            else {
                process(node.getBase(), builder);
            }
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<QualifiedName> builder)
        {
            builder.add(QualifiedName.of(node.getValue()));
            return null;
        }
    }
}
