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

package org.apache.doris.sql.planner.temp;

import avro.shaded.com.google.common.collect.Maps;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.planner.SimplePlanRewriter;
import org.apache.doris.sql.planner.Symbol;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.VariablesExtractor;
import org.apache.doris.sql.planner.optimizations.JoinNodeUtils;
import org.apache.doris.sql.planner.optimizations.PlanOptimizer;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.SemiJoinNode;
import org.apache.doris.sql.planner.plan.SortNode;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.OriginalExpressionUtils;
import org.apache.doris.sql.tree.BooleanLiteral;
import org.apache.doris.sql.tree.ComparisonExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.LongLiteral;
import org.apache.doris.sql.tree.NodeRef;
import org.apache.doris.sql.tree.SymbolReference;
import org.apache.doris.sql.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.ExpressionUtils.combineConjuncts;
import static org.apache.doris.sql.ExpressionUtils.extractConjuncts;
import static org.apache.doris.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static org.apache.doris.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static org.apache.doris.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static org.apache.doris.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static org.apache.doris.sql.planner.plan.JoinNode.Type.FULL;
import static org.apache.doris.sql.planner.plan.JoinNode.Type.INNER;
import static org.apache.doris.sql.planner.plan.JoinNode.Type.LEFT;
import static org.apache.doris.sql.planner.plan.JoinNode.Type.RIGHT;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class PredicatePushDown
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final LiteralEncoder literalEncoder;
    private final EffectivePredicateExtractor effectivePredicateExtractor;
    private final SqlParser sqlParser;

    public PredicatePushDown(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
        this.effectivePredicateExtractor = new EffectivePredicateExtractor(new ExpressionDomainTranslator(literalEncoder));
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public LogicalPlanNode optimize(LogicalPlanNode plan,
                                    Session session,
                                    TypeProvider types,
                                    VariableAllocator variableAllocator,
                                    IdGenerator<PlanNodeId> idAllocator,
                                    WarningCollector warningCollector) {
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(
                new Rewriter(variableAllocator, idAllocator, metadata, literalEncoder, effectivePredicateExtractor, sqlParser, session, types),
                plan,
                TRUE_LITERAL);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Expression>
    {
        private final PlanVariableAllocator variableAllocator;
        private final IdGenerator<PlanNodeId>  idAllocator;
        private final Metadata metadata;
        private final LiteralEncoder literalEncoder;
        private final EffectivePredicateExtractor effectivePredicateExtractor;
        private final SqlParser sqlParser;
        private final Session session;
        private final TypeProvider types;
        private final ExpressionEquivalence expressionEquivalence;

        private Rewriter(
                VariableAllocator variableAllocator,
                IdGenerator<PlanNodeId>  idAllocator,
                Metadata metadata,
                LiteralEncoder literalEncoder,
                EffectivePredicateExtractor effectivePredicateExtractor,
                SqlParser sqlParser,
                Session session,
                TypeProvider types)
        {
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.literalEncoder = requireNonNull(literalEncoder, "literalEncoder is null");
            this.effectivePredicateExtractor = requireNonNull(effectivePredicateExtractor, "effectivePredicateExtractor is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.expressionEquivalence = new ExpressionEquivalence(metadata, sqlParser);
        }

        @Override
        public LogicalPlanNode visitPlan(LogicalPlanNode node, RewriteContext<Expression> context)
        {
            LogicalPlanNode rewrittenNode = context.defaultRewrite(node, TRUE_LITERAL);
            if (!context.get().equals(TRUE_LITERAL)) {
                // Drop in a FilterNode b/c we cannot push our predicate down any further
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, castToRowExpression(context.get()));
            }
            return rewrittenNode;
        }

        @Override
        public LogicalPlanNode visitProject(ProjectNode node, RewriteContext<Expression> context)
        {
            Set<VariableReferenceExpression> deterministicVariables = node.getAssignments().entrySet().stream()
                    .filter(entry -> ExpressionDeterminismEvaluator.isDeterministic(castToExpression(entry.getValue())))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            Predicate<Expression> deterministic = conjunct -> deterministicVariables.containsAll(VariablesExtractor.extractUnique(conjunct, types));

            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(deterministic));

            // Push down conjuncts from the inherited predicate that only depend on deterministic assignments with
            // certain limitations.
            List<Expression> deterministicConjuncts = conjuncts.get(true);

            // We partition the expressions in the deterministicConjuncts into two lists, and only inline the
            // expressions that are in the inlining targets list.
            Map<Boolean, List<Expression>> inlineConjuncts = deterministicConjuncts.stream()
                    .collect(Collectors.partitioningBy(expression -> isInliningCandidate(expression, node)));

            List<Expression> inlinedDeterministicConjuncts = inlineConjuncts.get(true).stream()
                    .map(entry -> ExpressionVariableInliner.inlineVariables(Maps.transformValues(node.getAssignments().getMap(), OriginalExpressionUtils::castToExpression), entry, types))
                    .collect(Collectors.toList());

            LogicalPlanNode rewrittenNode = context.defaultRewrite(node, combineConjuncts(inlinedDeterministicConjuncts));

            // All deterministic conjuncts that contains non-inlining targets, and non-deterministic conjuncts,
            // if any, will be in the filter node.
            List<Expression> nonInliningConjuncts = inlineConjuncts.get(false);
            nonInliningConjuncts.addAll(conjuncts.get(false));

            if (!nonInliningConjuncts.isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, castToRowExpression(combineConjuncts(nonInliningConjuncts)));
            }

            return rewrittenNode;
        }

        private boolean isInliningCandidate(Expression expression, ProjectNode node)
        {
            // TryExpressions should not be pushed down. However they are now being handled as lambda
            // passed to a FunctionCall now and should not affect predicate push down. So we want to make
            // sure the conjuncts are not TryExpressions.
            verify(AstUtils.preOrder(expression).noneMatch(TryExpression.class::isInstance));

            // candidate symbols for inlining are
            //   1. references to simple constants
            //   2. references to complex expressions that appear only once
            // which come from the node, as opposed to an enclosing scope.
            Set<VariableReferenceExpression> childOutputSet = ImmutableSet.copyOf(node.getOutputVariables());
            Map<VariableReferenceExpression, Long> dependencies = VariablesExtractor.extractAll(expression, types).stream()
                    .filter(childOutputSet::contains)
                    .collect(Collectors.groupingBy(identity(), Collectors.counting()));

            return dependencies.entrySet().stream()
                    .allMatch(entry -> entry.getValue() == 1 || castToExpression(node.getAssignments().get(entry.getKey())) instanceof Literal);
        }

        @Override
        public LogicalPlanNode visitSort(SortNode node, RewriteContext<Expression> context)
        {
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public LogicalPlanNode visitFilter(FilterNode node, RewriteContext<Expression> context)
        {
            LogicalPlanNode rewrittenPlan = context.rewrite(node.getSource(), combineConjuncts(castToExpression(node.getPredicate()), context.get()));
            if (!(rewrittenPlan instanceof FilterNode)) {
                return rewrittenPlan;
            }

            FilterNode rewrittenFilterNode = (FilterNode) rewrittenPlan;
            if (!areExpressionsEquivalent(castToExpression(rewrittenFilterNode.getPredicate()), castToExpression(node.getPredicate()))
                    || node.getSource() != rewrittenFilterNode.getSource()) {
                return rewrittenPlan;
            }

            return node;
        }

        @Override
        public LogicalPlanNode visitJoin(JoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();

            // See if we can rewrite outer joins in terms of a plain inner join
            node = tryNormalizeToOuterToInnerJoin(node, inheritedPredicate);

            Expression leftEffectivePredicate = effectivePredicateExtractor.extract(node.getLeft(), types);
            Expression rightEffectivePredicate = effectivePredicateExtractor.extract(node.getRight(), types);
            Expression joinPredicate = extractJoinPredicate(node);

            Expression leftPredicate;
            Expression rightPredicate;
            Expression postJoinPredicate;
            Expression newJoinPredicate;

            switch (node.getType()) {
                case INNER:
                    InnerJoinPushDownResult innerJoinPushDownResult = processInnerJoin(inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            node.getLeft().getOutputVariables());
                    leftPredicate = innerJoinPushDownResult.getLeftPredicate();
                    rightPredicate = innerJoinPushDownResult.getRightPredicate();
                    postJoinPredicate = innerJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = innerJoinPushDownResult.getJoinPredicate();
                    break;
                case LEFT:
                    OuterJoinPushDownResult leftOuterJoinPushDownResult = processLimitedOuterJoin(inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            node.getLeft().getOutputVariables());
                    leftPredicate = leftOuterJoinPushDownResult.getOuterJoinPredicate();
                    rightPredicate = leftOuterJoinPushDownResult.getInnerJoinPredicate();
                    postJoinPredicate = leftOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = leftOuterJoinPushDownResult.getJoinPredicate();
                    break;
                case RIGHT:
                    OuterJoinPushDownResult rightOuterJoinPushDownResult = processLimitedOuterJoin(inheritedPredicate,
                            rightEffectivePredicate,
                            leftEffectivePredicate,
                            joinPredicate,
                            node.getRight().getOutputVariables());
                    leftPredicate = rightOuterJoinPushDownResult.getInnerJoinPredicate();
                    rightPredicate = rightOuterJoinPushDownResult.getOuterJoinPredicate();
                    postJoinPredicate = rightOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = rightOuterJoinPushDownResult.getJoinPredicate();
                    break;
                case FULL:
                    leftPredicate = TRUE_LITERAL;
                    rightPredicate = TRUE_LITERAL;
                    postJoinPredicate = inheritedPredicate;
                    newJoinPredicate = joinPredicate;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }

            newJoinPredicate = simplifyExpression(newJoinPredicate);
            // TODO: find a better way to directly optimize FALSE LITERAL in join predicate
            if (newJoinPredicate.equals(BooleanLiteral.FALSE_LITERAL)) {
                newJoinPredicate = new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new LongLiteral("0"), new LongLiteral("1"));
            }

            LogicalPlanNode leftSource = context.rewrite(node.getLeft(), leftPredicate);
            LogicalPlanNode rightSource = context.rewrite(node.getRight(), rightPredicate);

            LogicalPlanNode output = node;

            // Create identity projections for all existing symbols
            Assignments.Builder leftProjections = Assignments.builder()
                    .putAll(identityAssignmentsAsSymbolReferences(node.getLeft().getOutputVariables()));

            Assignments.Builder rightProjections = Assignments.builder()
                    .putAll(identityAssignmentsAsSymbolReferences(node.getRight().getOutputVariables()));

            // Create new projections for the new join clauses
            List<JoinNode.EquiJoinClause> equiJoinClauses = new ArrayList<>();
            ImmutableList.Builder<Expression> joinFilterBuilder = ImmutableList.builder();
            for (Expression conjunct : extractConjuncts(newJoinPredicate)) {
                if (joinEqualityExpression(node.getLeft().getOutputVariables()).test(conjunct)) {
                    ComparisonExpression equality = (ComparisonExpression) conjunct;

                    boolean alignedComparison = Iterables.all(VariablesExtractor.extractUnique(equality.getLeft(), types), in(node.getLeft().getOutputVariables()));
                    Expression leftExpression = (alignedComparison) ? equality.getLeft() : equality.getRight();
                    Expression rightExpression = (alignedComparison) ? equality.getRight() : equality.getLeft();

                    VariableReferenceExpression leftVariable = variableForExpression(leftExpression);
                    if (!node.getLeft().getOutputVariables().contains(leftVariable)) {
                        leftProjections.put(leftVariable, castToRowExpression(leftExpression));
                    }

                    VariableReferenceExpression rightVariable = variableForExpression(rightExpression);
                    if (!node.getRight().getOutputVariables().contains(rightVariable)) {
                        rightProjections.put(rightVariable, castToRowExpression(rightExpression));
                    }

                    equiJoinClauses.add(new JoinNode.EquiJoinClause(leftVariable, rightVariable));
                }
                else {
                    joinFilterBuilder.add(conjunct);
                }
            }

            Optional<Expression> newJoinFilter = Optional.of(combineConjuncts(joinFilterBuilder.build()));
            if (newJoinFilter.get() == TRUE_LITERAL) {
                newJoinFilter = Optional.empty();
            }

            if (node.getType() == INNER && newJoinFilter.isPresent() && equiJoinClauses.isEmpty()) {
                // if we do not have any equi conjunct we do not pushdown non-equality condition into
                // inner join, so we plan execution as nested-loops-join followed by filter instead
                // hash join.
                // todo: remove the code when we have support for filter function in nested loop join
                postJoinPredicate = combineConjuncts(postJoinPredicate, newJoinFilter.get());
                newJoinFilter = Optional.empty();
            }

            boolean filtersEquivalent =
                    newJoinFilter.isPresent() == node.getFilter().isPresent() &&
                            (!newJoinFilter.isPresent() || areExpressionsEquivalent(newJoinFilter.get(), castToExpression(node.getFilter().get())));

            if (leftSource != node.getLeft() ||
                    rightSource != node.getRight() ||
                    !filtersEquivalent ||
                    !ImmutableSet.copyOf(equiJoinClauses).equals(ImmutableSet.copyOf(node.getCriteria()))) {
                leftSource = new ProjectNode(idAllocator.getNextId(), leftSource, leftProjections.build());
                rightSource = new ProjectNode(idAllocator.getNextId(), rightSource, rightProjections.build());

                // if the distribution type is already set, make sure that changes from PredicatePushDown
                // don't make the join node invalid.
                Optional<JoinNode.DistributionType> distributionType = node.getDistributionType();
                if (node.getDistributionType().isPresent()) {
                    if (node.getType().mustPartition()) {
                        distributionType = Optional.of(PARTITIONED);
                    }
                    if (node.getType().mustReplicate(equiJoinClauses)) {
                        distributionType = Optional.of(REPLICATED);
                    }
                }

                output = new JoinNode(
                        node.getId(),
                        node.getType(),
                        leftSource,
                        rightSource,
                        equiJoinClauses,
                        ImmutableList.<VariableReferenceExpression>builder()
                                .addAll(leftSource.getOutputVariables())
                                .addAll(rightSource.getOutputVariables())
                                .build(),
                        newJoinFilter.map(OriginalExpressionUtils::castToRowExpression),
                        node.getLeftHashVariable(),
                        node.getRightHashVariable(),
                        distributionType);
            }

            if (!postJoinPredicate.equals(TRUE_LITERAL)) {
                output = new FilterNode(idAllocator.getNextId(), output, castToRowExpression(postJoinPredicate));
            }

            if (!node.getOutputVariables().equals(output.getOutputVariables())) {
                output = new ProjectNode(idAllocator.getNextId(), output, identityAssignmentsAsSymbolReferences(node.getOutputVariables()));
            }

            return output;
        }

        private VariableReferenceExpression variableForExpression(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                return new VariableReferenceExpression(((SymbolReference) expression).getName(), extractType(expression));
            }

            return variableAllocator.newVariable(expression, extractType(expression));
        }

        private OuterJoinPushDownResult processLimitedOuterJoin(Expression inheritedPredicate, Expression outerEffectivePredicate, Expression innerEffectivePredicate, Expression joinPredicate, Collection<VariableReferenceExpression> outerVariables)
        {
            checkArgument(Iterables.all(VariablesExtractor.extractUnique(outerEffectivePredicate, types), in(outerVariables)), "outerEffectivePredicate must only contain variables from outerVariables");
            checkArgument(Iterables.all(VariablesExtractor.extractUnique(innerEffectivePredicate, types), not(in(outerVariables))), "innerEffectivePredicate must not contain variables from outerVariables");

            ImmutableList.Builder<Expression> outerPushdownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> innerPushdownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> postJoinConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.builder();

            // Strip out non-deterministic conjuncts
            postJoinConjuncts.addAll(filter(extractConjuncts(inheritedPredicate), not(ExpressionDeterminismEvaluator::isDeterministic)));
            inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

            outerEffectivePredicate = filterDeterministicConjuncts(outerEffectivePredicate);
            innerEffectivePredicate = filterDeterministicConjuncts(innerEffectivePredicate);
            joinConjuncts.addAll(filter(extractConjuncts(joinPredicate), not(ExpressionDeterminismEvaluator::isDeterministic)));
            joinPredicate = filterDeterministicConjuncts(joinPredicate);

            // Generate equality inferences
            EqualityInference inheritedInference = createEqualityInference(inheritedPredicate);
            EqualityInference outerInference = createEqualityInference(inheritedPredicate, outerEffectivePredicate);

            EqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(in(outerVariables), types);
            Expression outerOnlyInheritedEqualities = combineConjuncts(equalityPartition.getScopeEqualities());
            EqualityInference potentialNullSymbolInference = createEqualityInference(outerOnlyInheritedEqualities, outerEffectivePredicate, innerEffectivePredicate, joinPredicate);

            // See if we can push inherited predicates down
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression outerRewritten = outerInference.rewriteExpression(conjunct, in(outerVariables), types);
                if (outerRewritten != null) {
                    outerPushdownConjuncts.add(outerRewritten);

                    // A conjunct can only be pushed down into an inner side if it can be rewritten in terms of the outer side
                    Expression innerRewritten = potentialNullSymbolInference.rewriteExpression(outerRewritten, not(in(outerVariables)), types);
                    if (innerRewritten != null) {
                        innerPushdownConjuncts.add(innerRewritten);
                    }
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            }
            // Add the equalities from the inferences back in
            outerPushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            // See if we can push down any outer effective predicates to the inner side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(outerEffectivePredicate)) {
                Expression rewritten = potentialNullSymbolInference.rewriteExpression(conjunct, not(in(outerVariables)), types);
                if (rewritten != null) {
                    innerPushdownConjuncts.add(rewritten);
                }
            }

            // See if we can push down join predicates to the inner side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(joinPredicate)) {
                Expression innerRewritten = potentialNullSymbolInference.rewriteExpression(conjunct, not(in(outerVariables)), types);
                if (innerRewritten != null) {
                    innerPushdownConjuncts.add(innerRewritten);
                }
                else {
                    joinConjuncts.add(conjunct);
                }
            }

            // Push outer and join equalities into the inner side. For example:
            // SELECT * FROM nation LEFT OUTER JOIN region ON nation.regionkey = region.regionkey and nation.name = region.name WHERE nation.name = 'blah'

            EqualityInference potentialNullSymbolInferenceWithoutInnerInferred = createEqualityInference(outerOnlyInheritedEqualities, outerEffectivePredicate, joinPredicate);
            innerPushdownConjuncts.addAll(potentialNullSymbolInferenceWithoutInnerInferred.generateEqualitiesPartitionedBy(not(in(outerVariables)), types).getScopeEqualities());

            // TODO: we can further improve simplifying the equalities by considering other relationships from the outer side
            EqualityInference.EqualityPartition joinEqualityPartition = createEqualityInference(joinPredicate).generateEqualitiesPartitionedBy(not(in(outerVariables)), types);
            innerPushdownConjuncts.addAll(joinEqualityPartition.getScopeEqualities());
            joinConjuncts.addAll(joinEqualityPartition.getScopeComplementEqualities())
                    .addAll(joinEqualityPartition.getScopeStraddlingEqualities());

            return new OuterJoinPushDownResult(combineConjuncts(outerPushdownConjuncts.build()),
                    combineConjuncts(innerPushdownConjuncts.build()),
                    combineConjuncts(joinConjuncts.build()),
                    combineConjuncts(postJoinConjuncts.build()));
        }

        private static class OuterJoinPushDownResult
        {
            private final Expression outerJoinPredicate;
            private final Expression innerJoinPredicate;
            private final Expression joinPredicate;
            private final Expression postJoinPredicate;

            private OuterJoinPushDownResult(Expression outerJoinPredicate, Expression innerJoinPredicate, Expression joinPredicate, Expression postJoinPredicate)
            {
                this.outerJoinPredicate = outerJoinPredicate;
                this.innerJoinPredicate = innerJoinPredicate;
                this.joinPredicate = joinPredicate;
                this.postJoinPredicate = postJoinPredicate;
            }

            private Expression getOuterJoinPredicate()
            {
                return outerJoinPredicate;
            }

            private Expression getInnerJoinPredicate()
            {
                return innerJoinPredicate;
            }

            public Expression getJoinPredicate()
            {
                return joinPredicate;
            }

            private Expression getPostJoinPredicate()
            {
                return postJoinPredicate;
            }
        }

        private InnerJoinPushDownResult processInnerJoin(Expression inheritedPredicate, Expression leftEffectivePredicate, Expression rightEffectivePredicate, Expression joinPredicate, Collection<VariableReferenceExpression> leftVariables)
        {
            checkArgument(Iterables.all(VariablesExtractor.extractUnique(leftEffectivePredicate, types), in(leftVariables)), "leftEffectivePredicate must only contain variables from leftVariables");
            checkArgument(Iterables.all(VariablesExtractor.extractUnique(rightEffectivePredicate, types), not(in(leftVariables))), "rightEffectivePredicate must not contain variables from leftVariables");

            ImmutableList.Builder<Expression> leftPushDownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> rightPushDownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.builder();

            // Strip out non-deterministic conjuncts
            joinConjuncts.addAll(filter(extractConjuncts(inheritedPredicate), not(ExpressionDeterminismEvaluator::isDeterministic)));
            inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

            joinConjuncts.addAll(filter(extractConjuncts(joinPredicate), not(ExpressionDeterminismEvaluator::isDeterministic)));
            joinPredicate = filterDeterministicConjuncts(joinPredicate);

            leftEffectivePredicate = filterDeterministicConjuncts(leftEffectivePredicate);
            rightEffectivePredicate = filterDeterministicConjuncts(rightEffectivePredicate);

            // Generate equality inferences
            EqualityInference allInference = createEqualityInference(inheritedPredicate, leftEffectivePredicate, rightEffectivePredicate, joinPredicate);
            EqualityInference allInferenceWithoutLeftInferred = createEqualityInference(inheritedPredicate, rightEffectivePredicate, joinPredicate);
            EqualityInference allInferenceWithoutRightInferred = createEqualityInference(inheritedPredicate, leftEffectivePredicate, joinPredicate);

            // Sort through conjuncts in inheritedPredicate that were not used for inference
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression leftRewrittenConjunct = allInference.rewriteExpression(conjunct, in(leftVariables), types);
                if (leftRewrittenConjunct != null) {
                    leftPushDownConjuncts.add(leftRewrittenConjunct);
                }

                Expression rightRewrittenConjunct = allInference.rewriteExpression(conjunct, not(in(leftVariables)), types);
                if (rightRewrittenConjunct != null) {
                    rightPushDownConjuncts.add(rightRewrittenConjunct);
                }

                // Drop predicate after join only if unable to push down to either side
                if (leftRewrittenConjunct == null && rightRewrittenConjunct == null) {
                    joinConjuncts.add(conjunct);
                }
            }

            // See if we can push the right effective predicate to the left side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(rightEffectivePredicate)) {
                Expression rewritten = allInference.rewriteExpression(conjunct, in(leftVariables), types);
                if (rewritten != null) {
                    leftPushDownConjuncts.add(rewritten);
                }
            }

            // See if we can push the left effective predicate to the right side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(leftEffectivePredicate)) {
                Expression rewritten = allInference.rewriteExpression(conjunct, not(in(leftVariables)), types);
                if (rewritten != null) {
                    rightPushDownConjuncts.add(rewritten);
                }
            }

            // See if we can push any parts of the join predicates to either side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(joinPredicate)) {
                Expression leftRewritten = allInference.rewriteExpression(conjunct, in(leftVariables), types);
                if (leftRewritten != null) {
                    leftPushDownConjuncts.add(leftRewritten);
                }

                Expression rightRewritten = allInference.rewriteExpression(conjunct, not(in(leftVariables)), types);
                if (rightRewritten != null) {
                    rightPushDownConjuncts.add(rightRewritten);
                }

                if (leftRewritten == null && rightRewritten == null) {
                    joinConjuncts.add(conjunct);
                }
            }

            // Add equalities from the inference back in
            leftPushDownConjuncts.addAll(allInferenceWithoutLeftInferred.generateEqualitiesPartitionedBy(in(leftVariables), types).getScopeEqualities());
            rightPushDownConjuncts.addAll(allInferenceWithoutRightInferred.generateEqualitiesPartitionedBy(not(in(leftVariables)), types).getScopeEqualities());
            joinConjuncts.addAll(allInference.generateEqualitiesPartitionedBy(in(leftVariables)::apply, types).getScopeStraddlingEqualities()); // scope straddling equalities get dropped in as part of the join predicate

            return new InnerJoinPushDownResult(combineConjuncts(leftPushDownConjuncts.build()), combineConjuncts(rightPushDownConjuncts.build()), combineConjuncts(joinConjuncts.build()), TRUE_LITERAL);
        }

        private static class InnerJoinPushDownResult
        {
            private final Expression leftPredicate;
            private final Expression rightPredicate;
            private final Expression joinPredicate;
            private final Expression postJoinPredicate;

            private InnerJoinPushDownResult(Expression leftPredicate, Expression rightPredicate, Expression joinPredicate, Expression postJoinPredicate)
            {
                this.leftPredicate = leftPredicate;
                this.rightPredicate = rightPredicate;
                this.joinPredicate = joinPredicate;
                this.postJoinPredicate = postJoinPredicate;
            }

            private Expression getLeftPredicate()
            {
                return leftPredicate;
            }

            private Expression getRightPredicate()
            {
                return rightPredicate;
            }

            private Expression getJoinPredicate()
            {
                return joinPredicate;
            }

            private Expression getPostJoinPredicate()
            {
                return postJoinPredicate;
            }
        }

        private static Expression extractJoinPredicate(JoinNode joinNode)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (JoinNode.EquiJoinClause equiJoinClause : joinNode.getCriteria()) {
                builder.add(JoinNodeUtils.toExpression(equiJoinClause));
            }
            joinNode.getFilter().map(OriginalExpressionUtils::castToExpression).ifPresent(builder::add);
            return combineConjuncts(builder.build());
        }

        private Type extractType(Expression expression)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, metadata, sqlParser, types, expression, emptyList(), /* parameters have already been replaced */WarningCollector.NOOP);
            return expressionTypes.get(NodeRef.of(expression));
        }

        private JoinNode tryNormalizeToOuterToInnerJoin(JoinNode node, Expression inheritedPredicate)
        {
            checkArgument(EnumSet.of(INNER, RIGHT, LEFT, FULL).contains(node.getType()), "Unsupported join type: %s", node.getType());

            if (node.getType() == INNER) {
                return node;
            }

            if (node.getType() == FULL) {
                boolean canConvertToLeftJoin = canConvertOuterToInner(node.getLeft().getOutputVariables(), inheritedPredicate);
                boolean canConvertToRightJoin = canConvertOuterToInner(node.getRight().getOutputVariables(), inheritedPredicate);
                if (!canConvertToLeftJoin && !canConvertToRightJoin) {
                    return node;
                }
                if (canConvertToLeftJoin && canConvertToRightJoin) {
                    return new JoinNode(node.getId(), INNER, node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputVariables(), node.getFilter(), node.getLeftHashVariable(), node.getRightHashVariable(), node.getDistributionType());
                }
                else {
                    return new JoinNode(node.getId(), canConvertToLeftJoin ? LEFT : RIGHT,
                            node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputVariables(), node.getFilter(), node.getLeftHashVariable(), node.getRightHashVariable(), node.getDistributionType());
                }
            }

            if (node.getType() == LEFT && !canConvertOuterToInner(node.getRight().getOutputVariables(), inheritedPredicate) ||
                    node.getType() == RIGHT && !canConvertOuterToInner(node.getLeft().getOutputVariables(), inheritedPredicate)) {
                return node;
            }
            return new JoinNode(node.getId(), INNER, node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputVariables(), node.getFilter(), node.getLeftHashVariable(), node.getRightHashVariable(), node.getDistributionType());
        }

        private boolean canConvertOuterToInner(List<VariableReferenceExpression> innerVariablesForOuterJoin, Expression inheritedPredicate)
        {
            Set<VariableReferenceExpression> innerVariables = ImmutableSet.copyOf(innerVariablesForOuterJoin);
            for (Expression conjunct : extractConjuncts(inheritedPredicate)) {
                if (ExpressionDeterminismEvaluator.isDeterministic(conjunct)) {
                    // Ignore a conjunct for this test if we can not deterministically get responses from it
                    Object response = nullInputEvaluator(innerVariables, conjunct);
                    if (response == null || response instanceof NullLiteral || Boolean.FALSE.equals(response)) {
                        // If there is a single conjunct that returns FALSE or NULL given all NULL inputs for the inner side symbols of an outer join
                        // then this conjunct removes all effects of the outer join, and effectively turns this into an equivalent of an inner join.
                        // So, let's just rewrite this join as an INNER join
                        return true;
                    }
                }
            }
            return false;
        }

        // Temporary implementation for joins because the SimplifyExpressions optimizers can not run properly on join clauses
        private Expression simplifyExpression(Expression expression)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                    session,
                    metadata,
                    sqlParser,
                    types,
                    expression,
                    emptyList(), /* parameters have already been replaced */
                    WarningCollector.NOOP);
            ExpressionInterpreter optimizer = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes);
            return literalEncoder.toExpression(optimizer.optimize(NoOpVariableResolver.INSTANCE), expressionTypes.get(NodeRef.of(expression)));
        }

        private boolean areExpressionsEquivalent(Expression leftExpression, Expression rightExpression)
        {
            return expressionEquivalence.areExpressionsEquivalent(session, leftExpression, rightExpression, types);
        }

        /**
         * Evaluates an expression's response to binding the specified input symbols to NULL
         */
        private Object nullInputEvaluator(final Collection<VariableReferenceExpression> nullVariables, Expression expression)
        {
            Set<String> nullVariableNames = nullVariables.stream()
                    .map(VariableReferenceExpression::getName)
                    .collect(toImmutableSet());
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                    session,
                    metadata,
                    sqlParser,
                    types,
                    expression,
                    emptyList(), /* parameters have already been replaced */
                    WarningCollector.NOOP);
            return ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes)
                    .optimize(variable -> nullVariableNames.contains(variable.getName()) ? null : new Symbol(variable.getName()).toSymbolReference());
        }

        private Predicate<Expression> joinEqualityExpression(final Collection<VariableReferenceExpression> leftVariables)
        {
            return expression -> {
                // At this point in time, our join predicates need to be deterministic
                if (isDeterministic(expression) && expression instanceof ComparisonExpression) {
                    ComparisonExpression comparison = (ComparisonExpression) expression;
                    if (comparison.getOperator() == ComparisonExpression.Operator.EQUAL) {
                        Set<VariableReferenceExpression> variables1 = VariablesExtractor.extractUnique(comparison.getLeft(), types);
                        Set<VariableReferenceExpression> variables2 = VariablesExtractor.extractUnique(comparison.getRight(), types);
                        if (variables1.isEmpty() || variables2.isEmpty()) {
                            return false;
                        }
                        return (Iterables.all(variables1, in(leftVariables)) && Iterables.all(variables2, not(in(leftVariables)))) ||
                                (Iterables.all(variables2, in(leftVariables)) && Iterables.all(variables1, not(in(leftVariables))));
                    }
                }
                return false;
            };
        }

        @Override
        public LogicalPlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();
            if (!extractConjuncts(inheritedPredicate).contains(new SymbolReference(node.getSemiJoinOutput().getName()))) {
                return visitNonFilteringSemiJoin(node, context);
            }
            return visitFilteringSemiJoin(node, context);
        }

        private LogicalPlanNode visitNonFilteringSemiJoin(SemiJoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();
            List<Expression> sourceConjuncts = new ArrayList<>();
            List<Expression> postJoinConjuncts = new ArrayList<>();

            // TODO: see if there are predicates that can be inferred from the semi join output

            LogicalPlanNode rewrittenFilteringSource = context.defaultRewrite(node.getFilteringSource(), TRUE_LITERAL);

            // Push inheritedPredicates down to the source if they don't involve the semi join output
            EqualityInference inheritedInference = createEqualityInference(inheritedPredicate);
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression rewrittenConjunct = inheritedInference.rewriteExpressionAllowNonDeterministic(conjunct, in(node.getSource().getOutputVariables()), types);
                // Since each source row is reflected exactly once in the output, ok to push non-deterministic predicates down
                if (rewrittenConjunct != null) {
                    sourceConjuncts.add(rewrittenConjunct);
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            }

            // Add the inherited equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(in(node.getSource()
                    .getOutputVariables())::apply, types);
            sourceConjuncts.addAll(equalityPartition.getScopeEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            LogicalPlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(sourceConjuncts));

            LogicalPlanNode output = node;
            if (rewrittenSource != node.getSource() || rewrittenFilteringSource != node.getFilteringSource()) {
                output = new SemiJoinNode(node.getId(), rewrittenSource, rewrittenFilteringSource, node.getSourceJoinVariable(), node.getFilteringSourceJoinVariable(), node.getSemiJoinOutput(), node.getSourceHashVariable(), node.getFilteringSourceHashVariable(), node.getDistributionType());
            }
            if (!postJoinConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, castToRowExpression(combineConjuncts(postJoinConjuncts)));
            }
            return output;
        }

        private LogicalPlanNode visitFilteringSemiJoin(SemiJoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();
            Expression deterministicInheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);
            Expression sourceEffectivePredicate = filterDeterministicConjuncts(effectivePredicateExtractor.extract(node.getSource(), types));
            Expression filteringSourceEffectivePredicate = filterDeterministicConjuncts(effectivePredicateExtractor.extract(node.getFilteringSource(), types));
            Expression joinExpression = new ComparisonExpression(
                    ComparisonExpression.Operator.EQUAL,
                    new SymbolReference(node.getSourceJoinVariable().getName()),
                    new SymbolReference(node.getFilteringSourceJoinVariable().getName()));

            List<VariableReferenceExpression> sourceVariables = node.getSource().getOutputVariables();
            List<VariableReferenceExpression> filteringSourceVariables = node.getFilteringSource().getOutputVariables();

            List<Expression> sourceConjuncts = new ArrayList<>();
            List<Expression> filteringSourceConjuncts = new ArrayList<>();
            List<Expression> postJoinConjuncts = new ArrayList<>();

            // Generate equality inferences
            EqualityInference allInference = createEqualityInference(deterministicInheritedPredicate, sourceEffectivePredicate, filteringSourceEffectivePredicate, joinExpression);
            EqualityInference allInferenceWithoutSourceInferred = createEqualityInference(deterministicInheritedPredicate, filteringSourceEffectivePredicate, joinExpression);
            EqualityInference allInferenceWithoutFilteringSourceInferred = createEqualityInference(deterministicInheritedPredicate, sourceEffectivePredicate, joinExpression);

            // Push inheritedPredicates down to the source if they don't involve the semi join output
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                Expression rewrittenConjunct = allInference.rewriteExpressionAllowNonDeterministic(conjunct, in(sourceVariables), types);
                // Since each source row is reflected exactly once in the output, ok to push non-deterministic predicates down
                if (rewrittenConjunct != null) {
                    sourceConjuncts.add(rewrittenConjunct);
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            }

            // Push inheritedPredicates down to the filtering source if possible
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(deterministicInheritedPredicate)) {
                Expression rewrittenConjunct = allInference.rewriteExpression(conjunct, in(filteringSourceVariables), types);
                // We cannot push non-deterministic predicates to filtering side. Each filtering side row have to be
                // logically reevaluated for each source row.
                if (rewrittenConjunct != null) {
                    filteringSourceConjuncts.add(rewrittenConjunct);
                }
            }

            // move effective predicate conjuncts source <-> filter
            // See if we can push the filtering source effective predicate to the source side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(filteringSourceEffectivePredicate)) {
                Expression rewritten = allInference.rewriteExpression(conjunct, in(sourceVariables), types);
                if (rewritten != null) {
                    sourceConjuncts.add(rewritten);
                }
            }

            // See if we can push the source effective predicate to the filtering soruce side
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(sourceEffectivePredicate)) {
                Expression rewritten = allInference.rewriteExpression(conjunct, in(filteringSourceVariables), types);
                if (rewritten != null) {
                    filteringSourceConjuncts.add(rewritten);
                }
            }

            // Add equalities from the inference back in
            sourceConjuncts.addAll(allInferenceWithoutSourceInferred.generateEqualitiesPartitionedBy(in(sourceVariables), types).getScopeEqualities());
            filteringSourceConjuncts.addAll(allInferenceWithoutFilteringSourceInferred.generateEqualitiesPartitionedBy(in(filteringSourceVariables), types).getScopeEqualities());

            LogicalPlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(sourceConjuncts));
            LogicalPlanNode rewrittenFilteringSource = context.rewrite(node.getFilteringSource(), combineConjuncts(filteringSourceConjuncts));

            LogicalPlanNode output = node;
            if (rewrittenSource != node.getSource() || rewrittenFilteringSource != node.getFilteringSource()) {
                output = new SemiJoinNode(
                        node.getId(),
                        rewrittenSource,
                        rewrittenFilteringSource,
                        node.getSourceJoinVariable(),
                        node.getFilteringSourceJoinVariable(),
                        node.getSemiJoinOutput(),
                        node.getSourceHashVariable(),
                        node.getFilteringSourceHashVariable(),
                        node.getDistributionType());
            }
            if (!postJoinConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, castToRowExpression(combineConjuncts(postJoinConjuncts)));
            }
            return output;
        }

        @Override
        public LogicalPlanNode visitAggregation(AggregationNode node, RewriteContext<Expression> context)
        {
            if (node.hasEmptyGroupingSet()) {
                // TODO: in case of grouping sets, we should be able to push the filters over grouping keys below the aggregation
                // and also preserve the filter above the aggregation if it has an empty grouping set
                return visitPlan(node, context);
            }

            Expression inheritedPredicate = context.get();

            EqualityInference equalityInference = createEqualityInference(inheritedPredicate);

            List<Expression> pushdownConjuncts = new ArrayList<>();
            List<Expression> postAggregationConjuncts = new ArrayList<>();

            List<VariableReferenceExpression> groupingKeyVariables = node.getGroupingKeys();

            // Strip out non-deterministic conjuncts
            postAggregationConjuncts.addAll(ImmutableList.copyOf(filter(extractConjuncts(inheritedPredicate), not(ExpressionDeterminismEvaluator::isDeterministic))));
            inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

            // Sort non-equality predicates by those that can be pushed down and those that cannot
            for (Expression conjunct : EqualityInference.nonInferrableConjuncts(inheritedPredicate)) {
                if (node.getGroupIdVariable().isPresent() && VariablesExtractor.extractUnique(conjunct, types).contains(node.getGroupIdVariable().get())) {
                    // aggregation operator synthesizes outputs for group ids corresponding to the global grouping set (i.e., ()), so we
                    // need to preserve any predicates that evaluate the group id to run after the aggregation
                    // TODO: we should be able to infer if conditions on grouping() correspond to global grouping sets to determine whether
                    // we need to do this for each specific case
                    postAggregationConjuncts.add(conjunct);
                    continue;
                }

                Expression rewrittenConjunct = equalityInference.rewriteExpression(conjunct, in(groupingKeyVariables), types);
                if (rewrittenConjunct != null) {
                    pushdownConjuncts.add(rewrittenConjunct);
                }
                else {
                    postAggregationConjuncts.add(conjunct);
                }
            }

            // Add the equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = equalityInference.generateEqualitiesPartitionedBy(in(groupingKeyVariables)::apply, types);
            pushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postAggregationConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postAggregationConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            LogicalPlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(pushdownConjuncts));

            LogicalPlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = new AggregationNode(node.getId(),
                        rewrittenSource,
                        node.getAggregations(),
                        node.getGroupingSets(),
                        ImmutableList.of(),
                        node.getStep(),
                        node.getHashVariable(),
                        node.getGroupIdVariable());
            }
            if (!postAggregationConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, castToRowExpression(combineConjuncts(postAggregationConjuncts)));
            }
            return output;
        }

        @Override
        public LogicalPlanNode visitTableScan(TableScanNode node, RewriteContext<Expression> context)
        {
            Expression predicate = simplifyExpression(context.get());

            if (!TRUE_LITERAL.equals(predicate)) {
                return new FilterNode(idAllocator.getNextId(), node, castToRowExpression(predicate));
            }

            return node;
        }
    }
}
