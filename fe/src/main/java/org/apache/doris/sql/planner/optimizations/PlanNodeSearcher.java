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

import com.google.common.collect.ImmutableList;
import org.apache.doris.sql.planner.iterative.Lookup;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.iterative.Lookup.noLookup;
import static org.apache.doris.sql.planner.plan.ChildReplacer.replaceChildren;

public class PlanNodeSearcher
{
    public static PlanNodeSearcher searchFrom(LogicalPlanNode node)
    {
        return searchFrom(node, noLookup());
    }

    public static PlanNodeSearcher searchFrom(LogicalPlanNode node, Lookup lookup)
    {
        return new PlanNodeSearcher(node, lookup);
    }

    private final LogicalPlanNode node;
    private final Lookup lookup;
    private Predicate<LogicalPlanNode> where = alwaysTrue();
    private Predicate<LogicalPlanNode> recurseOnlyWhen = alwaysTrue();

    private PlanNodeSearcher(LogicalPlanNode node, Lookup lookup)
    {
        this.node = requireNonNull(node, "node is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
    }

    public PlanNodeSearcher where(Predicate<LogicalPlanNode> where)
    {
        this.where = requireNonNull(where, "where is null");
        return this;
    }

    public PlanNodeSearcher recurseOnlyWhen(Predicate<LogicalPlanNode> skipOnly)
    {
        this.recurseOnlyWhen = requireNonNull(skipOnly, "recurseOnlyWhen is null");
        return this;
    }

    public <T extends LogicalPlanNode> Optional<T> findFirst()
    {
        return findFirstRecursive(node);
    }

    private <T extends LogicalPlanNode> Optional<T> findFirstRecursive(LogicalPlanNode node)
    {
        node = lookup.resolve(node);

        if (where.test(node)) {
            return Optional.of((T) node);
        }
        if (recurseOnlyWhen.test(node)) {
            for (LogicalPlanNode source : node.getSources()) {
                Optional<T> found = findFirstRecursive(source);
                if (found.isPresent()) {
                    return found;
                }
            }
        }
        return Optional.empty();
    }

    public <T extends LogicalPlanNode> Optional<T> findSingle()
    {
        List<T> all = findAll();
        switch (all.size()) {
            case 0:
                return Optional.empty();
            case 1:
                return Optional.of(all.get(0));
            default:
                throw new IllegalStateException("Multiple nodes found");
        }
    }

    public <T extends LogicalPlanNode> List<T> findAll()
    {
        ImmutableList.Builder<T> nodes = ImmutableList.builder();
        findAllRecursive(node, nodes);
        return nodes.build();
    }

    public <T extends LogicalPlanNode> T findOnlyElement()
    {
        return getOnlyElement(findAll());
    }

    public <T extends LogicalPlanNode> T findOnlyElement(T defaultValue)
    {
        List<T> all = findAll();
        if (all.size() == 0) {
            return defaultValue;
        }
        return getOnlyElement(all);
    }

    private <T extends LogicalPlanNode> void findAllRecursive(LogicalPlanNode node, ImmutableList.Builder<T> nodes)
    {
        node = lookup.resolve(node);

        if (where.test(node)) {
            nodes.add((T) node);
        }
        if (recurseOnlyWhen.test(node)) {
            for (LogicalPlanNode source : node.getSources()) {
                findAllRecursive(source, nodes);
            }
        }
    }

    public LogicalPlanNode removeAll()
    {
        return removeAllRecursive(node);
    }

    private LogicalPlanNode removeAllRecursive(LogicalPlanNode node)
    {
        node = lookup.resolve(node);

        if (where.test(node)) {
            checkArgument(
                    node.getSources().size() == 1,
                    "Unable to remove plan node as it contains 0 or more than 1 children");
            return node.getSources().get(0);
        }
        if (recurseOnlyWhen.test(node)) {
            List<LogicalPlanNode> sources = node.getSources().stream()
                    .map(this::removeAllRecursive)
                    .collect(Collectors.toList());
            return replaceChildren(node, sources);
        }
        return node;
    }

    public LogicalPlanNode removeFirst()
    {
        return removeFirstRecursive(node);
    }

    private LogicalPlanNode removeFirstRecursive(LogicalPlanNode node)
    {
        node = lookup.resolve(node);

        if (where.test(node)) {
            checkArgument(
                    node.getSources().size() == 1,
                    "Unable to remove plan node as it contains 0 or more than 1 children");
            return node.getSources().get(0);
        }
        if (recurseOnlyWhen.test(node)) {
            List<LogicalPlanNode> sources = node.getSources();
            if (sources.isEmpty()) {
                return node;
            }
            else if (sources.size() == 1) {
                return replaceChildren(node, ImmutableList.of(removeFirstRecursive(sources.get(0))));
            }
            else {
                throw new IllegalArgumentException("Unable to remove first node when a node has multiple children, use removeAll instead");
            }
        }
        return node;
    }

    public LogicalPlanNode replaceAll(LogicalPlanNode newLogicalPlanNode)
    {
        return replaceAllRecursive(node, newLogicalPlanNode);
    }

    private LogicalPlanNode replaceAllRecursive(LogicalPlanNode node, LogicalPlanNode nodeToReplace)
    {
        node = lookup.resolve(node);

        if (where.test(node)) {
            return nodeToReplace;
        }
        if (recurseOnlyWhen.test(node)) {
            List<LogicalPlanNode> sources = node.getSources().stream()
                    .map(source -> replaceAllRecursive(source, nodeToReplace))
                    .collect(Collectors.toList());
            return replaceChildren(node, sources);
        }
        return node;
    }

    public LogicalPlanNode replaceFirst(LogicalPlanNode newLogicalPlanNode)
    {
        return replaceFirstRecursive(node, newLogicalPlanNode);
    }

    private LogicalPlanNode replaceFirstRecursive(LogicalPlanNode node, LogicalPlanNode nodeToReplace)
    {
        node = lookup.resolve(node);

        if (where.test(node)) {
            return nodeToReplace;
        }
        List<LogicalPlanNode> sources = node.getSources();
        if (sources.isEmpty()) {
            return node;
        }
        else if (sources.size() == 1) {
            return replaceChildren(node, ImmutableList.of(replaceFirstRecursive(node, sources.get(0))));
        }
        else {
            throw new IllegalArgumentException("Unable to replace first node when a node has multiple children, use replaceAll instead");
        }
    }

    public boolean matches()
    {
        return findFirst().isPresent();
    }

    public int count()
    {
        return findAll().size();
    }
}
