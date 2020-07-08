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

import com.google.common.collect.ImmutableList;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.matching.Capture;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.doris.sql.planner.iterative.matching.Capture.newCapture;
import static org.apache.doris.sql.planner.iterative.rule.Util.pruneInputs;
import static org.apache.doris.sql.planner.plan.Patterns.project;
import static org.apache.doris.sql.planner.plan.Patterns.source;

/**
 * @param <N> The node type to look for under the ProjectNode
 * Looks for a Project parent over a N child, such that the parent doesn't use all the output columns of the child.
 * Given that situation, invokes the pushDownProjectOff helper to possibly rewrite the child to produce fewer outputs.
 */
public abstract class ProjectOffPushDownRule<N extends LogicalPlanNode>
        implements Rule<ProjectNode>
{
    private final Capture<N> targetCapture = newCapture();

    private final Pattern<N> targetPattern;

    protected ProjectOffPushDownRule(Pattern<N> targetPattern)
    {
        this.targetPattern = targetPattern;
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(targetPattern.capturedAs(targetCapture)));
    }

    @Override
    public Result apply(ProjectNode parent, Captures captures, Context context)
    {
        N targetNode = captures.get(targetCapture);

        return pruneInputs(
                targetNode.getOutputVariables(),
                parent.getAssignments().getExpressions().stream().collect(toImmutableList()),
                context.getVariableAllocator().getTypes())
                .flatMap(prunedOutputs -> this.pushDownProjectOff(context.getIdAllocator(), context.getVariableAllocator(), targetNode, prunedOutputs))
                .map(newChild -> parent.replaceChildren(ImmutableList.of(newChild)))
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }

    protected abstract Optional<LogicalPlanNode> pushDownProjectOff(IdGenerator<PlanNodeId> idAllocator, VariableAllocator variableAllocator, N targetNode, Set<VariableReferenceExpression> referencedOutputs);
}
