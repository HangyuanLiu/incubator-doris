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
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.analyzer.ResolvedField;
import org.apache.doris.sql.tree.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Keeps track of fields and expressions and their mapping to symbols in the current plan
 */
class TranslationMap
{
    // all expressions are rewritten in terms of fields declared by this relation plan
    private final RelationPlan rewriteBase;
    private final Analysis analysis;

    // current mappings of underlying field -> symbol for translating direct field references
    private final VariableReferenceExpression[] fieldVariables;

    // current mappings of sub-expressions -> symbol
    private final Map<Expression, VariableReferenceExpression> expressionToVariables = new HashMap<>();
    private final Map<Expression, Expression> expressionToExpressions = new HashMap<>();

    public TranslationMap(RelationPlan rewriteBase, Analysis analysis)
    {
        this.rewriteBase = requireNonNull(rewriteBase, "rewriteBase is null");
        this.analysis = requireNonNull(analysis, "analysis is null");

        fieldVariables = new VariableReferenceExpression[rewriteBase.getFieldMappings().size()];
    }

    public RelationPlan getRelationPlan()
    {
        return rewriteBase;
    }

    public Analysis getAnalysis()
    {
        return analysis;
    }

    public void setFieldMappings(List<VariableReferenceExpression> variables)
    {
        for (int i = 0; i < variables.size(); i++) {
            this.fieldVariables[i] = variables.get(i);
        }
    }

    public void copyMappingsFrom(TranslationMap other)
    {
        expressionToVariables.putAll(other.expressionToVariables);
        expressionToExpressions.putAll(other.expressionToExpressions);
        System.arraycopy(other.fieldVariables, 0, fieldVariables, 0, other.fieldVariables.length);
    }

    public void putExpressionMappingsFrom(TranslationMap other)
    {
        expressionToVariables.putAll(other.expressionToVariables);
        expressionToExpressions.putAll(other.expressionToExpressions);
    }

    public Expression rewrite(Expression expression)
    {
        // first, translate names from sql-land references to plan symbols
        Expression mapped = translateNamesToSymbols(expression);

        // then rewrite subexpressions in terms of the current mappings
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (expressionToVariables.containsKey(node)) {
                    return new SymbolReference(expressionToVariables.get(node).getName());
                }

                Expression translated = expressionToExpressions.getOrDefault(node, node);
                return treeRewriter.defaultRewrite(translated, context);
            }
        }, mapped);
    }

    public void put(Expression expression, VariableReferenceExpression variable)
    {
        if (expression instanceof FieldReference) {
            int fieldIndex = ((FieldReference) expression).getFieldIndex();
            fieldVariables[fieldIndex] = variable;
            expressionToVariables.put(new SymbolReference(rewriteBase.getVariable(fieldIndex).getName()), variable);
            return;
        }

        Expression translated = translateNamesToSymbols(expression);
        expressionToVariables.put(translated, variable);

        // also update the field mappings if this expression is a field reference
        rewriteBase.getScope().tryResolveField(expression)
                .filter(ResolvedField::isLocal)
                .ifPresent(field -> fieldVariables[field.getHierarchyFieldIndex()] = variable);
    }

    public boolean containsSymbol(Expression expression)
    {
        if (expression instanceof FieldReference) {
            int field = ((FieldReference) expression).getFieldIndex();
            return fieldVariables[field] != null;
        }

        Expression translated = translateNamesToSymbols(expression);
        return expressionToVariables.containsKey(translated);
    }

    public VariableReferenceExpression get(Expression expression)
    {
        if (expression instanceof FieldReference) {
            int field = ((FieldReference) expression).getFieldIndex();
            return fieldVariables[field];
        }

        Expression translated = translateNamesToSymbols(expression);
        if (!expressionToVariables.containsKey(translated)) {
            return get(expressionToExpressions.get(translated));
        }

        return expressionToVariables.get(translated);
    }

    public void put(Expression expression, Expression rewritten)
    {
        expressionToExpressions.put(translateNamesToSymbols(expression), rewritten);
    }

    private Expression translateNamesToSymbols(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Expression rewrittenExpression = treeRewriter.defaultRewrite(node, context);
                return coerceIfNecessary(node, rewrittenExpression);
            }

            @Override
            public Expression rewriteFieldReference(FieldReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                VariableReferenceExpression variable = rewriteBase.getVariable(node.getFieldIndex());
                return new SymbolReference(variable.getName());
            }

            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return rewriteExpressionWithResolvedName(node);
            }

            private Expression rewriteExpressionWithResolvedName(Expression node)
            {
                return getVariable(rewriteBase, node)
                        .map(variable -> coerceIfNecessary(node, new SymbolReference(variable.getName())))
                        .orElse(coerceIfNecessary(node, node));
            }

            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (analysis.isColumnReference(node)) {
                    Optional<ResolvedField> resolvedField = rewriteBase.getScope().tryResolveField(node);
                    if (resolvedField.isPresent()) {
                        if (resolvedField.get().isLocal()) {
                            return getVariable(rewriteBase, node)
                                    .map(variable -> coerceIfNecessary(node, new SymbolReference(variable.getName())))
                                    .orElseThrow(() -> new IllegalStateException("No symbol mapping for node " + node));
                        }
                    }
                    // do not rewrite outer references, it will be handled in outer scope planner
                    return node;
                }
                return rewriteExpression(node, context, treeRewriter);
            }

            private Expression coerceIfNecessary(Expression original, Expression rewritten)
            {
                /*
                Type coercion = analysis.getCoercion(original);
                if (coercion != null) {
                    rewritten = new Cast(
                            rewritten,
                            coercion.getTypeSignature().toString(),
                            false,
                            analysis.isTypeOnlyCoercion(original));
                }

                 */
                return rewritten;
            }
        }, expression, null);
    }

    private Optional<VariableReferenceExpression> getVariable(RelationPlan plan, Expression expression)
    {
        if (!analysis.isColumnReference(expression)) {
            // Expression can be a reference to lambda argument (or DereferenceExpression based on lambda argument reference).
            // In such case, the expression might still be resolvable with plan.getScope() but we should not resolve it.
            return Optional.empty();
        }
        return plan.getScope()
                .tryResolveField(expression)
                .filter(ResolvedField::isLocal)
                .map(field -> requireNonNull(plan.getFieldMappings().get(field.getHierarchyFieldIndex())));
    }
}
