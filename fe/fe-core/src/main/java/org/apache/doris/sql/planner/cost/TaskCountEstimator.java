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

import javax.inject.Inject;

import java.util.Set;
import java.util.function.IntSupplier;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TaskCountEstimator
{

    private IntSupplier numberOfNodes;
    /*
    public TaskCountEstimator(NodeSchedulerConfig nodeSchedulerConfig, InternalNodeManager nodeManager)
    {
        requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null");
        requireNonNull(nodeManager, "nodeManager is null");
        this.numberOfNodes = () -> {
            Set<InternalNode> activeNodes = nodeManager.getAllNodes().getActiveNodes();
            if (nodeSchedulerConfig.isIncludeCoordinator()) {
                return activeNodes.size();
            }
            return toIntExact(activeNodes.stream()
                    .filter(node -> !node.isCoordinator())
                    .count());
        };
    }
    */
    public TaskCountEstimator(IntSupplier numberOfNodes)
    {
        this.numberOfNodes = requireNonNull(numberOfNodes, "numberOfNodes is null");
    }

    public TaskCountEstimator(){}

    public int estimateSourceDistributedTaskCount()
    {
        //FIXME
        //return numberOfNodes.getAsInt();
        return 3;
    }

}