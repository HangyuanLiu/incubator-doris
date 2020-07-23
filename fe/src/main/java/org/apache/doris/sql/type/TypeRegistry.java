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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

import static org.apache.doris.sql.type.CharType.createCharType;
import static org.apache.doris.sql.type.DateType.DATE;
import static org.apache.doris.sql.type.DecimalType.createDecimalType;
import static org.apache.doris.sql.type.IntegerType.INTEGER;
import static org.apache.doris.sql.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static org.apache.doris.sql.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static org.apache.doris.sql.type.TimestampType.TIMESTAMP;
import static org.apache.doris.sql.type.TinyintType.TINYINT;
import static org.apache.doris.sql.type.UnknownType.UNKNOWN;
import static org.apache.doris.sql.type.BigintType.BIGINT;
import static org.apache.doris.sql.type.BooleanType.BOOLEAN;
import static org.apache.doris.sql.type.DoubleType.DOUBLE;
import static org.apache.doris.sql.type.VarcharType.createUnboundedVarcharType;
import static org.apache.doris.sql.type.VarcharType.createVarcharType;

@ThreadSafe
public final class TypeRegistry
        implements TypeManager
{
    private final ConcurrentMap<TypeSignature, Type> types = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ParametricType> parametricTypes = new ConcurrentHashMap<>();
    private final LoadingCache<TypeSignature, Type> parametricTypeCache;

    public TypeRegistry()
    {

        // Manually register UNKNOWN type without a verifyTypeClass call since it is a special type that can not be used by functions
        this.types.put(UNKNOWN.getTypeSignature(), UNKNOWN);

        // always add the built-in types; Presto will not function without these
        addType(BOOLEAN);
        addType(TINYINT);
        addType(BIGINT);
        addType(INTEGER);
        addType(DOUBLE);
        addType(DATE);
        addType(TIMESTAMP);
        addType(INTERVAL_YEAR_MONTH);
        addType(INTERVAL_DAY_TIME);
        addParametricType(VarcharParametricType.VARCHAR);
        addParametricType(CharParametricType.CHAR);
        addParametricType(DecimalParametricType.DECIMAL);

        parametricTypeCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(CacheLoader.from(this::instantiateParametricType));
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        Type type = types.get(signature);

        if (type == null) {
            try {
                return parametricTypeCache.getUnchecked(signature);
            }
            catch (UncheckedExecutionException e) {
                throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        }


        return type;
    }

    private Type instantiateParametricType(TypeSignature signature)
    {
        List<TypeParameter> parameters = new ArrayList<>();

        for (TypeSignatureParameter parameter : signature.getParameters()) {
            TypeParameter typeParameter = TypeParameter.of(parameter, this);
            parameters.add(typeParameter);
        }

        ParametricType parametricType = parametricTypes.get(signature.getBase().toLowerCase(Locale.ENGLISH));
        if (parametricType == null) {
            throw new IllegalArgumentException("Unknown type " + signature);
        }

        Type instantiatedType = parametricType.createType(this, parameters);

        // TODO: reimplement this check? Currently "varchar(Integer.MAX_VALUE)" fails with "varchar"
        //checkState(instantiatedType.equalsSignature(signature), "Instantiated parametric type name (%s) does not match expected name (%s)", instantiatedType, signature);
        return instantiatedType;
    }


    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.copyOf(types.values());
    }

    @Override
    public boolean isTypeOnlyCoercion(Type source, Type result)
    {
        if (source.equals(result)) {
            return true;
        }

        if (!canCoerce(source, result)) {
            return false;
        }

        return false;
    }

    @Override
    public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
    {
        TypeCompatibility compatibility = compatibility(firstType, secondType);
        if (!compatibility.isCompatible()) {
            return Optional.empty();
        }
        return Optional.of(compatibility.getCommonSuperType());
    }

    @Override
    public boolean canCoerce(Type fromType, Type toType)
    {
        TypeCompatibility typeCompatibility = compatibility(fromType, toType);
        return typeCompatibility.isCoercible();
    }

    private TypeCompatibility compatibility(Type fromType, Type toType)
    {
        if (fromType.equals(toType)) {
            return TypeCompatibility.compatible(toType, true);
        }

        if (fromType.equals(UnknownType.UNKNOWN)) {
            return TypeCompatibility.compatible(toType, true);
        }

        if (toType.equals(UnknownType.UNKNOWN)) {
            return TypeCompatibility.compatible(fromType, false);
        }

        String fromTypeBaseName = fromType.getTypeSignature().getBase();
        String toTypeBaseName = toType.getTypeSignature().getBase();

        if (fromTypeBaseName.equals(toTypeBaseName)) {
            if (fromTypeBaseName.equals(StandardTypes.DECIMAL)) {
                Type commonSuperType = getCommonSuperTypeForDecimal((DecimalType) fromType, (DecimalType) toType);
                return TypeCompatibility.compatible(commonSuperType, commonSuperType.equals(toType));
            }
            if (fromTypeBaseName.equals(StandardTypes.VARCHAR)) {
                Type commonSuperType = getCommonSuperTypeForVarchar((VarcharType) fromType, (VarcharType) toType);
                return TypeCompatibility.compatible(commonSuperType, commonSuperType.equals(toType));
            }
            if (fromTypeBaseName.equals(StandardTypes.CHAR)) { //&& !featuresConfig.isLegacyCharToVarcharCoercion()) {
                Type commonSuperType = getCommonSuperTypeForChar((CharType) fromType, (CharType) toType);
                return TypeCompatibility.compatible(commonSuperType, commonSuperType.equals(toType));
            }
            return TypeCompatibility.incompatible();
        }

        Optional<Type> coercedType = coerceTypeBase(fromType, toType.getTypeSignature().getBase());
        if (coercedType.isPresent()) {
            return compatibility(coercedType.get(), toType);
        }

        coercedType = coerceTypeBase(toType, fromType.getTypeSignature().getBase());
        if (coercedType.isPresent()) {
            TypeCompatibility typeCompatibility = compatibility(fromType, coercedType.get());
            if (!typeCompatibility.isCompatible()) {
                return TypeCompatibility.incompatible();
            }
            return TypeCompatibility.compatible(typeCompatibility.getCommonSuperType(), false);
        }

        return TypeCompatibility.incompatible();
    }

    private static Type getCommonSuperTypeForDecimal(DecimalType firstType, DecimalType secondType)
    {
        int targetScale = Math.max(firstType.getScale(), secondType.getScale());
        int targetPrecision = Math.max(firstType.getPrecision() - firstType.getScale(), secondType.getPrecision() - secondType.getScale()) + targetScale;
        //we allow potential loss of precision here. Overflow checking is done in operators.
        targetPrecision = Math.min(38, targetPrecision);
        return createDecimalType(targetPrecision, targetScale);
    }

    private static Type getCommonSuperTypeForVarchar(VarcharType firstType, VarcharType secondType)
    {
        if (firstType.isUnbounded() || secondType.isUnbounded()) {
            return createUnboundedVarcharType();
        }

        return createVarcharType(Math.max(firstType.getLength(), secondType.getLength()));
    }

    private static Type getCommonSuperTypeForChar(CharType firstType, CharType secondType)
    {
        return createCharType(Math.max(firstType.getLength(), secondType.getLength()));
    }

    public void addType(Type type)
    {
        requireNonNull(type, "type is null");
        Type existingType = types.putIfAbsent(type.getTypeSignature(), type);
        checkState(existingType == null || existingType.equals(type), "Type %s is already registered");
    }

    public void addParametricType(ParametricType parametricType)
    {
        String name = parametricType.getName().toLowerCase(Locale.ENGLISH);
        checkArgument(!parametricTypes.containsKey(name), "Parametric type already registered");
        parametricTypes.putIfAbsent(name, parametricType);
    }

    /**
     * coerceTypeBase and isCovariantParametrizedType defines all hand-coded rules for type coercion.
     * Other methods should reference these two functions instead of hand-code new rules.
     */
    @Override
    public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
    {
        String sourceTypeName = sourceType.getTypeSignature().getBase();
        if (sourceTypeName.equals(resultTypeBase)) {
            return Optional.of(sourceType);
        }

        switch (sourceTypeName) {
            case StandardTypes.INTEGER: {
                switch (resultTypeBase) {
                    case StandardTypes.BIGINT:
                        return Optional.of(BIGINT);
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    case StandardTypes.DECIMAL:
                        return Optional.of(createDecimalType(10, 0));
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.BIGINT: {
                switch (resultTypeBase) {
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    case StandardTypes.DECIMAL:
                        return Optional.of(createDecimalType(19, 0));
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.DATE: {
                switch (resultTypeBase) {
                    case StandardTypes.TIMESTAMP:
                        return Optional.of(TIMESTAMP);
                    default:
                        return Optional.empty();
                }
            }
            default:
                return Optional.empty();
        }
    }

    @Override
    public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes) {
        return null;
    }


    public static class TypeCompatibility
    {
        private final Optional<Type> commonSuperType;
        private final boolean coercible;

        // Do not call constructor directly. Use factory methods.
        private TypeCompatibility(Optional<Type> commonSuperType, boolean coercible)
        {
            // Assert that: coercible => commonSuperType.isPresent
            // The factory API is designed such that this is guaranteed.
            checkArgument(!coercible || commonSuperType.isPresent());

            this.commonSuperType = commonSuperType;
            this.coercible = coercible;
        }

        private static TypeCompatibility compatible(Type commonSuperType, boolean coercible)
        {
            return new TypeCompatibility(Optional.of(commonSuperType), coercible);
        }

        private static TypeCompatibility incompatible()
        {
            return new TypeCompatibility(Optional.empty(), false);
        }

        public boolean isCompatible()
        {
            return commonSuperType.isPresent();
        }

        public Type getCommonSuperType()
        {
            checkState(commonSuperType.isPresent(), "Types are not compatible");
            return commonSuperType.get();
        }

        public boolean isCoercible()
        {
            return coercible;
        }
    }
}
