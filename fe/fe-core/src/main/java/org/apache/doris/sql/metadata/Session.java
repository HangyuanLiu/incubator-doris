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
package org.apache.doris.sql.metadata;

import com.google.common.collect.ImmutableMap;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.sql.util.DataSize;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class Session
{
    public static final String DEFAULT_CLUSTER = "default_cluster";
    private final Catalog catalog;
    private final ConnectContext context;

    public Session(Catalog catalog, ConnectContext context) {
        this.catalog = catalog;
        this.context = context;
    }

    public Optional<String> getCluster() {
        return Optional.of(DEFAULT_CLUSTER);
    }

    public Optional<String> getSchema() {
        //return Optional.of(context.getDatabase());
        return Optional.of(context.getDatabase().substring(16));
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public ConnectorSession toConnectorSession(ConnectorId connectorId)
    {
        requireNonNull(connectorId, "connectorId is null");

        return new FullConnectorSession(
                this,
                connectorId);
    }

    public ConnectorSession toConnectorSession()
    {
        return new FullConnectorSession(this, new ConnectorId("0"));
    }


    public enum JoinDistributionType
    {
        BROADCAST,
        PARTITIONED,
        AUTOMATIC;

        public boolean canPartition()
        {
            return this == PARTITIONED || this == AUTOMATIC;
        }

        public boolean canReplicate()
        {
            return this == BROADCAST || this == AUTOMATIC;
        }
    }

    public static JoinDistributionType getJoinDistributionType(Session session) {
        //return JoinDistributionType.AUTOMATIC;
        return JoinDistributionType.BROADCAST;
    }

    public static Optional<DataSize> getJoinMaxBroadcastTableSize(Session session)
    {
        return Optional.of(new DataSize());
    }
}
