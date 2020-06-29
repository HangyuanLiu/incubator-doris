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
package org.apache.doris.sql.planner.plan;


import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.iterative.matching.Property;

import java.util.Optional;

import static org.apache.doris.sql.planner.iterative.matching.Pattern.typeOf;
import static org.apache.doris.sql.planner.iterative.matching.Property.optionalProperty;

public class Patterns
{
    private Patterns() {}

    public static Pattern<ProjectNode> project()
    {
        return typeOf(ProjectNode.class);
    }

    public static Pattern<FilterNode> filter()
    {
        return typeOf(FilterNode.class);
    }

    public static Pattern<LimitNode> limit()
    {
        return typeOf(LimitNode.class);
    }

    public static Pattern<SortNode> sort()
    {
        return typeOf(SortNode.class);
    }

    public static Property<LogicalPlanNode, LogicalPlanNode> source()
    {
        return optionalProperty("source", node -> node.getSources().size() == 1 ?
                Optional.of(node.getSources().get(0)) :
                Optional.empty());
    }
}
