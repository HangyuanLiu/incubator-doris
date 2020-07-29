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
package org.apache.doris.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.expressions.DefaultRowExpressionTraversalVisitor;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.planner.ExpressionVariableInliner;
import org.apache.doris.sql.planner.RowExpressionVariableInliner;
import org.apache.doris.sql.planner.VariablesExtractor;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.matching.Capture;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.ConstantExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.FunctionResolution;
import org.apache.doris.sql.relational.OriginalExpressionUtils;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.Literal;
import org.apache.doris.sql.tree.SymbolReference;
import org.apache.doris.sql.tree.TryExpression;
import org.apache.doris.sql.util.AstUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.apache.doris.sql.planner.iterative.matching.Capture.newCapture;
import static org.apache.doris.sql.planner.plan.AssignmentUtils.identityAsSymbolReference;
import static org.apache.doris.sql.planner.plan.AssignmentUtils.isIdentity;
import static org.apache.doris.sql.planner.plan.Patterns.project;
import static org.apache.doris.sql.planner.plan.Patterns.source;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.isExpression;

/**
 * Inlines expressions from a child project node into a parent project node
 * as long as they are simple constants, or they are referenced only once (to
 * avoid introducing duplicate computation) and the references don't appear
 * within a TRY block (to avoid changing semantics).
 */
public class InlineProjections
        implements Rule<ProjectNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(project().capturedAs(CHILD)));

    private final FunctionResolution functionResolution;

    public InlineProjections(FunctionManager functionManager)
    {
        this.functionResolution = new FunctionResolution(functionManager);
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode parent, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);

        Sets.SetView<VariableReferenceExpression> targets = extractInliningTargets(parent, child, context);
        if (targets.isEmpty()) {
            return Result.empty();
        }

        // inline the expressions
        Assignments assignments = child.getAssignments().filter(targets::contains);
        Map<VariableReferenceExpression, RowExpression> parentAssignments = parent.getAssignments()
                .entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> inlineReferences(entry.getValue(), assignments, context.getVariableAllocator().getTypes())));

        // Synthesize identity assignments for the inputs of expressions that were inlined
        // to place in the child projection.
        // If all assignments end up becoming identity assignments, they'll get pruned by
        // other rules
        Set<VariableReferenceExpression> inputs = child.getAssignments()
                .entrySet().stream()
                .filter(entry -> targets.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .flatMap(expression -> extractInputs(expression, context.getVariableAllocator().getTypes()).stream())
                .collect(toSet());

        Assignments.Builder childAssignments = Assignments.builder();
        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : child.getAssignments().entrySet()) {
            if (!targets.contains(assignment.getKey())) {
                childAssignments.put(assignment);
            }
        }

        boolean allTranslated = child.getAssignments().entrySet()
                .stream()
                .map(Map.Entry::getValue)
                .noneMatch(OriginalExpressionUtils::isExpression);

        for (VariableReferenceExpression input : inputs) {
            if (allTranslated) {
                childAssignments.put(input, input);
            }
            else {
                childAssignments.put(identityAsSymbolReference(input));
            }
        }

        return Result.ofPlanNode(
                new ProjectNode(
                        parent.getId(),
                        new ProjectNode(
                                child.getId(),
                                child.getSource(),
                                childAssignments.build()),
                        Assignments.copyOf(parentAssignments)));
    }

    private RowExpression inlineReferences(RowExpression expression, Assignments assignments, TypeProvider types)
    {
        if (isExpression(expression)) {
            Function<VariableReferenceExpression, Expression> mapping = variable -> {
                if (assignments.get(variable) == null) {
                    return new SymbolReference(variable.getName());
                }
                return castToExpression(assignments.get(variable));
            };
            return castToRowExpression(ExpressionVariableInliner.inlineVariables(mapping, castToExpression(expression), types));
        }
        return RowExpressionVariableInliner.inlineVariables(variable -> assignments.getMap().getOrDefault(variable, variable), expression);
    }

    private Sets.SetView<VariableReferenceExpression> extractInliningTargets(ProjectNode parent, ProjectNode child, Context context)
    {
        // candidates for inlining are
        //   1. references to simple constants
        //   2. references to complex expressions that
        //      a. are not inputs to try() expressions
        //      b. appear only once across all expressions
        //      c. are not identity projections
        // which come from the child, as opposed to an enclosing scope.

        Set<VariableReferenceExpression> childOutputSet = ImmutableSet.copyOf(child.getOutputVariables());
        TypeProvider types = context.getVariableAllocator().getTypes();

        Map<VariableReferenceExpression, Long> dependencies = parent.getAssignments()
                .getExpressions()
                .stream()
                .flatMap(expression -> extractInputs(expression, context.getVariableAllocator().getTypes()).stream())
                .filter(childOutputSet::contains)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        // find references to simple constants
        Set<VariableReferenceExpression> constants = dependencies.keySet().stream()
                .filter(input -> isConstant(child.getAssignments().get(input)))
                .collect(toSet());

        // exclude any complex inputs to TRY expressions. Inlining them would potentially
        // change the semantics of those expressions
        Set<VariableReferenceExpression> tryArguments = parent.getAssignments()
                .getExpressions().stream()
                .flatMap(expression -> extractTryArguments(expression, types).stream())
                .collect(toSet());

        Set<VariableReferenceExpression> singletons = dependencies.entrySet().stream()
                .filter(entry -> entry.getValue() == 1) // reference appears just once across all expressions in parent project node
                .filter(entry -> !tryArguments.contains(entry.getKey())) // they are not inputs to TRY. Otherwise, inlining might change semantics
                .filter(entry -> !isIdentity(child.getAssignments(), entry.getKey())) // skip identities, otherwise, this rule will keep firing forever
                .map(Map.Entry::getKey)
                .collect(toSet());

        return Sets.union(singletons, constants);
    }

    private Set<VariableReferenceExpression> extractTryArguments(RowExpression expression, TypeProvider types)
    {
        if (isExpression(expression)) {
            return AstUtils.preOrder(castToExpression(expression))
                    .filter(TryExpression.class::isInstance)
                    .map(TryExpression.class::cast)
                    .flatMap(tryExpression -> VariablesExtractor.extractAll(tryExpression, types).stream())
                    .collect(toSet());
        }
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        expression.accept(new DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>>()
        {
            @Override
            public Void visitCall(CallExpression call, ImmutableSet.Builder<VariableReferenceExpression> context)
            {
                if (functionResolution.isTryFunction(call.getFunctionHandle())) {
                    context.addAll(VariablesExtractor.extractAll(call));
                }
                return super.visitCall(call, context);
            }
        }, builder);
        return builder.build();
    }

    private static List<VariableReferenceExpression> extractInputs(RowExpression expression, TypeProvider types)
    {
        if (isExpression(expression)) {
            return VariablesExtractor.extractAll(castToExpression(expression), types);
        }
        return VariablesExtractor.extractAll(expression);
    }

    private static boolean isConstant(RowExpression expression)
    {
        if (isExpression(expression)) {
            return castToExpression(expression) instanceof Literal;
        }
        return expression instanceof ConstantExpression;
    }
}
