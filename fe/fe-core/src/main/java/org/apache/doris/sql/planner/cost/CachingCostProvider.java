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
package org.apache.doris.sql.planner.cost;

import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.iterative.GroupReference;
import org.apache.doris.sql.planner.iterative.Memo;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class CachingCostProvider
        implements CostProvider
{
    private final CostCalculator costCalculator;
    private final StatsProvider statsProvider;
    private final Optional<Memo> memo;
    private final Session session;

    private final Map<LogicalPlanNode, PlanCostEstimate> cache = new IdentityHashMap<>();

    public CachingCostProvider(CostCalculator costCalculator, StatsProvider statsProvider, Session session)
    {
        this(costCalculator, statsProvider, Optional.empty(), session);
    }

    public CachingCostProvider(CostCalculator costCalculator, StatsProvider statsProvider, Optional<Memo> memo, Session session)
    {
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
        this.memo = requireNonNull(memo, "memo is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public PlanCostEstimate getCost(LogicalPlanNode node)
    {
        //if (!isEnableStatsCalculator(session)) {
        //    return PlanCostEstimate.unknown();
        //}

        requireNonNull(node, "node is null");

        try {
            if (node instanceof GroupReference) {
                return getGroupCost((GroupReference) node);
            }

            PlanCostEstimate cost = cache.get(node);
            if (cost != null) {
                return cost;
            }

            cost = calculateCost(node);
            verify(cache.put(node, cost) == null, "Cost already set");
            return cost;
        }
        catch (RuntimeException e) {
            //if (isIgnoreStatsCalculatorFailures(session)) {
             //   return PlanCostEstimate.unknown();
            //}
            throw e;
        }
    }

    private PlanCostEstimate getGroupCost(GroupReference groupReference)
    {
        int group = groupReference.getGroupId();
        Memo memo = this.memo.orElseThrow(() -> new IllegalStateException("CachingCostProvider without memo cannot handle GroupReferences"));

        Optional<PlanCostEstimate> knownCost = memo.getCost(group);
        if (knownCost.isPresent()) {
            return knownCost.get();
        }

        PlanCostEstimate cost = calculateCost(memo.getNode(group));
        verify(!memo.getCost(group).isPresent(), "Group cost already set");
        memo.storeCost(group, cost);
        return cost;
    }

    private PlanCostEstimate calculateCost(LogicalPlanNode node)
    {
        return costCalculator.calculateCost(node, statsProvider, this, session);
    }
}
