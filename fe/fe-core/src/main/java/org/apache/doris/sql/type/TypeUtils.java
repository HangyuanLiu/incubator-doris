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
package org.apache.doris.sql.type;


import java.util.List;
import java.util.stream.Collectors;


public final class TypeUtils {
    public static final long NULL_HASH_CODE = 0;

    private TypeUtils() {
    }

    public static Type resolveType(TypeSignature typeName, TypeManager typeManager) {
        return typeManager.getType(typeName);
    }

    public static boolean isIntegralType(TypeSignature typeName, TypeManager typeManager) {
        switch (typeName.getBase()) {
            case StandardTypes.BIGINT:
            case StandardTypes.INTEGER:
            case StandardTypes.SMALLINT:
            case StandardTypes.TINYINT:
                return true;
            case StandardTypes.DECIMAL:
                DecimalType decimalType = (DecimalType) resolveType(typeName, typeManager);
                return decimalType.getScale() == 0;
            default:
                return false;
        }
    }

    public static List<Type> resolveTypes(List<TypeSignature> typeNames, TypeManager typeManager) {
        return typeNames.stream()
                .map((TypeSignature type) -> resolveType(type, typeManager))
                .collect(Collectors.toList());
    }
}