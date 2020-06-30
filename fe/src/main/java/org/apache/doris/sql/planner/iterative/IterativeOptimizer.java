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
package org.apache.doris.sql.planner.iterative;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.planner.RuleStatsRecorder;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.cost.CostCalculator;
import org.apache.doris.sql.planner.cost.CostProvider;
import org.apache.doris.sql.planner.cost.StatsCalculator;
import org.apache.doris.sql.planner.cost.StatsProvider;
import org.apache.doris.sql.planner.iterative.matching.Match;
import org.apache.doris.sql.planner.iterative.matching.Matcher;
import org.apache.doris.sql.planner.optimizations.PlanOptimizer;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class IterativeOptimizer
        implements PlanOptimizer
{
    private final RuleStatsRecorder stats;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final List<PlanOptimizer> legacyRules;
    private final RuleIndex ruleIndex;

    public IterativeOptimizer(RuleStatsRecorder stats, StatsCalculator statsCalculator, CostCalculator costCalculator, Set<Rule<?>> rules)
    {
        this(stats, statsCalculator, costCalculator, ImmutableList.of(), rules);
    }

    public IterativeOptimizer(RuleStatsRecorder stats, StatsCalculator statsCalculator, CostCalculator costCalculator, List<PlanOptimizer> legacyRules, Set<Rule<?>> newRules)
    {
        this.stats = null;
        this.statsCalculator = null;
        this.costCalculator = null;
        this.legacyRules = null;

        this.ruleIndex = RuleIndex.builder()
                .register(newRules)
                .build();

        //stats.registerAll(newRules);
    }

    @Override
    public LogicalPlanNode optimize(LogicalPlanNode plan,
                                    Session session,
                                    TypeProvider types,
                                    VariableAllocator variableAllocator,
                                    IdGenerator<PlanNodeId> idAllocator,
                                    WarningCollector warningCollector) {
        // only disable new rules if we have legacy rules to fall back to
        /*
        if (!SystemSessionProperties.isNewOptimizerEnabled(session) && !legacyRules.isEmpty()) {
            for (PlanOptimizer optimizer : legacyRules) {
                plan = optimizer.optimize(plan, session, types, idAllocator, warningCollector);
            }

            return plan;
        }

         */

        Memo memo = new Memo(idAllocator, plan);
        Lookup lookup = Lookup.from(planNode -> Stream.of(memo.resolve(planNode)));
        Matcher matcher = new PlanNodeMatcher(lookup);

        //Duration timeout = SystemSessionProperties.getOptimizerTimeout(session);
        Context context = new Context(memo, lookup, idAllocator, variableAllocator, System.nanoTime(), 30000, session, warningCollector);
        exploreGroup(memo.getRootGroup(), context, matcher);

        return memo.extract();
    }

    private boolean exploreGroup(int group, Context context, Matcher matcher)
    {
        // tracks whether this group or any children groups change as
        // this method executes
        boolean progress = exploreNode(group, context, matcher);

        while (exploreChildren(group, context, matcher)) {
            progress = true;

            // if children changed, try current group again
            // in case we can match additional rules
            if (!exploreNode(group, context, matcher)) {
                // no additional matches, so bail out
                break;
            }
        }

        return progress;
    }

    private boolean exploreNode(int group, Context context, Matcher matcher)
    {
        LogicalPlanNode node = context.memo.getNode(group);

        boolean done = false;
        boolean progress = false;

        while (!done) {
            context.checkTimeoutNotExhausted();

            done = true;
            Iterator<Rule<?>> possiblyMatchingRules = ruleIndex.getCandidates(node).iterator();
            while (possiblyMatchingRules.hasNext()) {
                Rule<?> rule = possiblyMatchingRules.next();

                if (!rule.isEnabled(context.session)) {
                    continue;
                }

                Rule.Result result = transform(node, rule, matcher, context);

                if (result.getTransformedPlan().isPresent()) {
                    node = context.memo.replace(group, result.getTransformedPlan().get(), rule.getClass().getName());

                    done = false;
                    progress = true;
                }
            }
        }

        return progress;
    }

    private <T> Rule.Result transform(LogicalPlanNode node, Rule<T> rule, Matcher matcher, Context context)
    {
        Rule.Result result;

        Match<T> match = matcher.match(rule.getPattern(), node);

        if (match.isEmpty()) {
            return Rule.Result.empty();
        }

        long duration;
        try {
            long start = System.nanoTime();
            result = rule.apply(match.value(), match.captures(), ruleContext(context));
            duration = System.nanoTime() - start;
        }
        catch (RuntimeException e) {
            //stats.recordFailure(rule);
            throw e;
        }
        //stats.record(rule, duration, !result.isEmpty());

        return result;
    }

    private boolean exploreChildren(int group, Context context, Matcher matcher)
    {
        boolean progress = false;

        LogicalPlanNode expression = context.memo.getNode(group);
        for (LogicalPlanNode child : expression.getSources()) {
            Preconditions.checkState(child instanceof GroupReference, "Expected child to be a group reference. Found: " + child.getClass().getName());

            if (exploreGroup(((GroupReference) child).getGroupId(), context, matcher)) {
                progress = true;
            }
        }

        return progress;
    }

    private Rule.Context ruleContext(Context context)
    {
        //StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, Optional.of(context.memo), context.lookup, context.session, context.variableAllocator.getTypes());
        //CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.of(context.memo), context.session);
        StatsProvider statsProvider = null;
        CostProvider costProvider = null;

        return new Rule.Context()
        {
            @Override
            public Lookup getLookup()
            {
                return context.lookup;
            }

            @Override
            public IdGenerator<PlanNodeId>  getIdAllocator()
            {
                return context.idAllocator;
            }

            @Override
            public VariableAllocator getVariableAllocator()
            {
                return context.variableAllocator;
            }

            @Override
            public Session getSession()
            {
                return context.session;
            }

            @Override
            public StatsProvider getStatsProvider()
            {
                return statsProvider;
            }

            @Override
            public CostProvider getCostProvider()
            {
                return costProvider;
            }

            @Override
            public void checkTimeoutNotExhausted()
            {
                context.checkTimeoutNotExhausted();
            }

            @Override
            public WarningCollector getWarningCollector()
            {
                return context.warningCollector;
            }
        };
    }

    private static class Context
    {
        private final Memo memo;
        private final Lookup lookup;
        private final IdGenerator<PlanNodeId> idAllocator;
        private final VariableAllocator variableAllocator;
        private final long startTimeInNanos;
        private final long timeoutInMilliseconds;
        private final Session session;
        private final WarningCollector warningCollector;

        public Context(
                Memo memo,
                Lookup lookup,
                IdGenerator<PlanNodeId>  idAllocator,
                VariableAllocator variableAllocator,
                long startTimeInNanos,
                long timeoutInMilliseconds,
                Session session,
                WarningCollector warningCollector)
        {
            checkArgument(timeoutInMilliseconds >= 0, "Timeout has to be a non-negative number [milliseconds]");

            this.memo = memo;
            this.lookup = lookup;
            this.idAllocator = idAllocator;
            this.variableAllocator = variableAllocator;
            this.startTimeInNanos = startTimeInNanos;
            this.timeoutInMilliseconds = timeoutInMilliseconds;
            this.session = session;
            this.warningCollector = warningCollector;
        }

        public void checkTimeoutNotExhausted()
        {
            /*
            if ((NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos)) >= timeoutInMilliseconds) {
                throw new PrestoException(OPTIMIZER_TIMEOUT, format("The optimizer exhausted the time limit of %d ms", timeoutInMilliseconds));
            }

             */
        }
    }
}
