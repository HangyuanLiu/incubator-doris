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


import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.optimizations.PlanOptimizer;
import org.apache.doris.sql.planner.plan.ApplyNode;
import org.apache.doris.sql.planner.plan.LateralJoinNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.apache.doris.sql.planner.optimizations.PlanNodeSearcher.searchFrom;

public class CheckSubqueryNodesAreRewritten
        implements PlanOptimizer
{
    @Override
    public LogicalPlanNode optimize(LogicalPlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        searchFrom(plan).where(ApplyNode.class::isInstance)
                .findFirst()
                .ifPresent(node -> {
                    ApplyNode applyNode = (ApplyNode) node;
                    error(applyNode.getCorrelation(), applyNode.getOriginSubqueryError());
                });

        searchFrom(plan).where(LateralJoinNode.class::isInstance)
                .findFirst()
                .ifPresent(node -> {
                    LateralJoinNode lateralJoinNode = (LateralJoinNode) node;
                    error(lateralJoinNode.getCorrelation(), lateralJoinNode.getOriginSubqueryError());
                });

        return plan;
    }

    private void error(List<VariableReferenceExpression> correlation, String originSubqueryError)
    {
        checkState(!correlation.isEmpty(), "All the non correlated subqueries should be rewritten at this point");
        //throw new Exception(UNSUPPORTED_SUBQUERY, format(originSubqueryError, "Given correlated subquery is not supported"));
    }
}
