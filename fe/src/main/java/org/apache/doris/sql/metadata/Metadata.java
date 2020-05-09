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

import java.util.Map;
import java.util.Optional;

public interface Metadata
{

    boolean schemaExists(Session session, CatalogSchemaName schema);
    /**
     * Returns a table handle for the specified table name.
     */
    Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName);

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    TableMetadata getTableMetadata(Session session, TableHandle tableHandle);

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle);
}
