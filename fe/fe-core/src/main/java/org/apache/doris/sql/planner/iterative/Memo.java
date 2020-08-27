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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.planner.cost.PlanCostEstimate;
import org.apache.doris.sql.planner.cost.PlanNodeStatsEstimate;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.iterative.Plans.resolveGroupReferences;

/**
 * Stores a plan in a form that's efficient to mutate locally (i.e. without
 * having to do full ancestor tree rewrites due to plan nodes being immutable).
 * <p>
 * Each node in a plan is placed in a group, and it's children are replaced with
 * symbolic references to the corresponding groups.
 * <p>
 * For example, a plan like:
 * <pre>
 *    A -> B -> C -> D
 *           \> E -> F
 * </pre>
 * would be stored as:
 * <pre>
 * root: G0
 *
 * G0 : { A -> G1 }
 * G1 : { B -> [G2, G3] }
 * G2 : { C -> G4 }
 * G3 : { E -> G5 }
 * G4 : { D }
 * G5 : { F }
 * </pre>
 * Groups are reference-counted, and groups that become unreachable from the root
 * due to mutations in a subtree get garbage-collected.
 */
public class Memo
{
    private static final int ROOT_GROUP_REF = 0;

    private final PlanNodeIdAllocator idAllocator;
    private final int rootGroup;

    private final Map<Integer, Group> groups = new HashMap<>();

    private int nextGroupId = ROOT_GROUP_REF + 1;

    public Memo(PlanNodeIdAllocator idAllocator, LogicalPlanNode plan)
    {
        this.idAllocator = idAllocator;
        rootGroup = insertRecursive(plan);
        groups.get(rootGroup).incomingReferences.add(ROOT_GROUP_REF);
    }

    public int getRootGroup()
    {
        return rootGroup;
    }

    private Group getGroup(int group)
    {
        checkArgument(groups.containsKey(group), "Invalid group: %s");
        return groups.get(group);
    }

    public LogicalPlanNode getNode(int group)
    {
        return getGroup(group).membership;
    }

    public LogicalPlanNode resolve(GroupReference groupReference)
    {
        return getNode(groupReference.getGroupId());
    }

    public LogicalPlanNode extract()
    {
        return extract(getNode(rootGroup));
    }

    private LogicalPlanNode extract(LogicalPlanNode node)
    {
        return resolveGroupReferences(node, Lookup.from(planNode -> Stream.of(this.resolve(planNode))));
    }

    public LogicalPlanNode replace(int group, LogicalPlanNode node, String reason)
    {
        LogicalPlanNode old = getGroup(group).membership;

        checkArgument(new HashSet<>(old.getOutputVariables()).equals(new HashSet<>(node.getOutputVariables())),
                "%s: transformed expression doesn't produce same outputs: %s vs %s");

        if (node instanceof GroupReference) {
            node = getNode(((GroupReference) node).getGroupId());
        }
        else {
            node = insertChildrenAndRewrite(node);
        }

        incrementReferenceCounts(node, group);
        getGroup(group).membership = node;
        decrementReferenceCounts(old, group);
        evictStatisticsAndCost(group);

        return node;
    }

    private void evictStatisticsAndCost(int group)
    {
        getGroup(group).stats = null;
        getGroup(group).cost = null;
        for (int parentGroup : getGroup(group).incomingReferences.elementSet()) {
            if (parentGroup != ROOT_GROUP_REF) {
                evictStatisticsAndCost(parentGroup);
            }
        }
    }

    public Optional<PlanNodeStatsEstimate> getStats(int group)
    {
        return Optional.ofNullable(getGroup(group).stats);
    }

    public void storeStats(int groupId, PlanNodeStatsEstimate stats)
    {
        Group group = getGroup(groupId);
        if (group.stats != null) {
            evictStatisticsAndCost(groupId); // cost is derived from stats, also needs eviction
        }
        group.stats = requireNonNull(stats, "stats is null");
    }

    public Optional<PlanCostEstimate> getCost(int group)
    {
        return Optional.ofNullable(getGroup(group).cost);
    }

    public void storeCost(int group, PlanCostEstimate cost)
    {
        getGroup(group).cost = requireNonNull(cost, "cost is null");
    }

    private void incrementReferenceCounts(LogicalPlanNode fromNode, int fromGroup)
    {
        Set<Integer> references = getAllReferences(fromNode);

        for (int group : references) {
            groups.get(group).incomingReferences.add(fromGroup);
        }
    }

    private void decrementReferenceCounts(LogicalPlanNode fromNode, int fromGroup)
    {
        Set<Integer> references = getAllReferences(fromNode);

        for (int group : references) {
            Group childGroup = groups.get(group);
            checkState(childGroup.incomingReferences.remove(fromGroup), "Reference to remove not found");

            if (childGroup.incomingReferences.isEmpty()) {
                deleteGroup(group);
            }
        }
    }

    private Set<Integer> getAllReferences(LogicalPlanNode node)
    {
        return node.getSources().stream()
                .map(GroupReference.class::cast)
                .map(GroupReference::getGroupId)
                .collect(Collectors.toSet());
    }

    private void deleteGroup(int group)
    {
        checkArgument(getGroup(group).incomingReferences.isEmpty(), "Cannot delete group that has incoming references");
        LogicalPlanNode deletedNode = groups.remove(group).membership;
        decrementReferenceCounts(deletedNode, group);
    }

    private LogicalPlanNode insertChildrenAndRewrite(LogicalPlanNode node)
    {
        return node.replaceChildren(
                node.getSources().stream()
                        .map(child -> new GroupReference(
                                idAllocator.getNextId(),
                                insertRecursive(child),
                                child.getOutputVariables()))
                        .collect(Collectors.toList()));
    }

    private int insertRecursive(LogicalPlanNode node)
    {
        if (node instanceof GroupReference) {
            return ((GroupReference) node).getGroupId();
        }

        int group = nextGroupId();
        LogicalPlanNode rewritten = insertChildrenAndRewrite(node);

        groups.put(group, Group.withMember(rewritten));
        incrementReferenceCounts(rewritten, group);

        return group;
    }

    private int nextGroupId()
    {
        return nextGroupId++;
    }

    public int getGroupCount()
    {
        return groups.size();
    }

    private static final class Group
    {
        static Group withMember(LogicalPlanNode member)
        {
            return new Group(member);
        }

        private LogicalPlanNode membership;
        private final Multiset<Integer> incomingReferences = HashMultiset.create();
        @Nullable
        private PlanNodeStatsEstimate stats;
        @Nullable
        private PlanCostEstimate cost;

        private Group(LogicalPlanNode member)
        {
            this.membership = requireNonNull(member, "member is null");
        }
    }
}