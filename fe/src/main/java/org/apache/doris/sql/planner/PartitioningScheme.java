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

import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class PartitioningScheme
{
    private final List<VariableReferenceExpression> outputLayout;
    private final Optional<VariableReferenceExpression> hashColumn;

    public PartitioningScheme (List<VariableReferenceExpression> outputLayout, Optional<VariableReferenceExpression> hashColumn) {
        this.outputLayout = outputLayout;
        this.hashColumn = hashColumn;
    }

    public List<VariableReferenceExpression> getOutputLayout() {
        return outputLayout;
    }

    public Optional<VariableReferenceExpression> getHashColumn() {
        return hashColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitioningScheme that = (PartitioningScheme) o;
        return Objects.equals(outputLayout, that.outputLayout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputLayout);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("outputLayout", outputLayout)
                .add("hashChannel", hashColumn)
                .toString();
    }
}
