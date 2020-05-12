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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.doris.sql.analyzer.SemanticException;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.QualifiedName;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static org.apache.doris.sql.analyzer.SemanticErrorCode.SCHEMA_NOT_SPECIFIED;

public final class MetadataUtil
{
    private MetadataUtil() {}

    public static void checkTableName(String catalogName, Optional<String> schemaName, Optional<String> tableName)
    {
        checkCatalogName(catalogName);
        schemaName.ifPresent(name -> checkLowerCase(name, "schemaName"));
        tableName.ifPresent(name -> checkLowerCase(name, "tableName"));

        checkArgument(schemaName.isPresent() || !tableName.isPresent(), "tableName specified but schemaName is missing");
    }

    public static String checkCatalogName(String catalogName)
    {
        return checkLowerCase(catalogName, "catalogName");
    }

    public static String checkSchemaName(String schemaName)
    {
        return checkLowerCase(schemaName, "schemaName");
    }

    public static String checkTableName(String tableName)
    {
        return checkLowerCase(tableName, "tableName");
    }

    public static void checkObjectName(String catalogName, String schemaName, String objectName)
    {
        checkLowerCase(catalogName, "catalogName");
        checkLowerCase(schemaName, "schemaName");
        checkLowerCase(objectName, "objectName");
    }

    public static String checkLowerCase(String value, String name)
    {
        if (value == null) {
            throw new NullPointerException(format("%s is null", name));
        }
        checkArgument(value.equals(value.toLowerCase(ENGLISH)), "%s is not lowercase: %s", name, value);
        return value;
    }

    public static QualifiedObjectName createQualifiedObjectName(Session session, Node node, QualifiedName name) {
        //requireNonNull(session, "session is null");
        requireNonNull(name, "name is null");
        if (name.getParts().size() > 3) {
            //throw new Exception(format("Too many dots in table name: %s", name));
        }

        List<String> parts = Lists.reverse(name.getParts());
        String objectName = parts.get(0);

        String schemaName = (parts.size() > 1) ? parts.get(1) : session.getSchema().orElseThrow(() ->
                new SemanticException(SCHEMA_NOT_SPECIFIED, node, "Schema must be specified when session schema is not set"));
        String catalogName = (parts.size() > 2) ? parts.get(2) : session.getCatalog().orElseThrow(() ->
                new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set"));

        return new QualifiedObjectName(catalogName, schemaName, objectName);
    }
}
