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
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.planner.plan.Ordering;
import org.apache.doris.sql.planner.plan.OrderingScheme;
import org.apache.doris.sql.planner.plan.SortOrder;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.OrderBy;
import org.apache.doris.sql.tree.SortItem;
import org.apache.doris.sql.tree.SymbolReference;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

import static com.google.common.collect.Streams.forEachPair;
import static org.apache.doris.sql.relational.Expressions.variable;

public class PlannerUtils
{
    private PlannerUtils() {}

    public static SortOrder toSortOrder(SortItem sortItem)
    {
        if (sortItem.getOrdering() == SortItem.Ordering.ASCENDING) {
            if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
                return SortOrder.ASC_NULLS_FIRST;
            }
            return SortOrder.ASC_NULLS_LAST;
        }
        if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
            return SortOrder.DESC_NULLS_FIRST;
        }
        return SortOrder.DESC_NULLS_LAST;
    }

    public static OrderingScheme toOrderingScheme(OrderBy orderBy, TypeProvider types)
    {
        return toOrderingScheme(
                orderBy.getSortItems().stream()
                        .map(SortItem::getSortKey)
                        .map(item -> {
                            checkArgument(item instanceof SymbolReference, "must be symbol reference");
                            return variable(((SymbolReference) item).getName(), types.get(item));
                        }).collect(Collectors.toList()),
                orderBy.getSortItems().stream()
                        .map(PlannerUtils::toSortOrder)
                        .collect(Collectors.toList()));
    }

    public static OrderingScheme toOrderingScheme(List<VariableReferenceExpression> orderingSymbols, List<SortOrder> sortOrders)
    {
        ImmutableList.Builder<Ordering> builder = ImmutableList.builder();

        // don't override existing keys, i.e. when "ORDER BY a ASC, a DESC" is specified
        Set<VariableReferenceExpression> keysSeen = new HashSet<>();

        forEachPair(orderingSymbols.stream(), sortOrders.stream(), (variable, sortOrder) -> {
            if (!keysSeen.contains(variable)) {
                keysSeen.add(variable);
                builder.add(new Ordering(variable, sortOrder));
            }
        });

        return new OrderingScheme(builder.build());
    }

    public static VariableReferenceExpression toVariableReference(Expression expression, TypeProvider types)
    {
        checkArgument(expression instanceof SymbolReference);
        return variable(((SymbolReference) expression).getName(), types.get(expression));
    }
}
