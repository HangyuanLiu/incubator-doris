package org.apache.doris.sql.function;

import org.apache.doris.sql.type.TypeSignature;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ParametricScalarImplementation
        implements ParametricImplementation {
    private final Signature signature;
    private final List<Optional<Class<?>>> argumentNativeContainerTypes; // argument native container type is Optional.empty() for function type
    private final Map<String, Class<?>> specializedTypeParameters;
    private final Class<?> returnNativeContainerType;


    public static final class SpecializedSignature
    {
        private final Signature signature;
        private final List<Optional<Class<?>>> argumentNativeContainerTypes;
        private final Map<String, Class<?>> specializedTypeParameters;
        private final Class<?> returnNativeContainerType;

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SpecializedSignature that = (SpecializedSignature) o;
            return Objects.equals(signature, that.signature) &&
                    Objects.equals(argumentNativeContainerTypes, that.argumentNativeContainerTypes) &&
                    Objects.equals(specializedTypeParameters, that.specializedTypeParameters) &&
                    Objects.equals(returnNativeContainerType, that.returnNativeContainerType);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(signature, argumentNativeContainerTypes, specializedTypeParameters, returnNativeContainerType);
        }

        private SpecializedSignature(
                Signature signature,
                List<Optional<Class<?>>> argumentNativeContainerTypes,
                Map<String, Class<?>> specializedTypeParameters,
                Class<?> returnNativeContainerType)
        {
            this.signature = signature;
            this.argumentNativeContainerTypes = argumentNativeContainerTypes;
            this.specializedTypeParameters = specializedTypeParameters;
            this.returnNativeContainerType = returnNativeContainerType;
        }
    }

    public static final class Parser
    {
        private final ScalarImplementationHeader header;
        private final boolean nullable;
        private final TypeSignature returnType;
        private final List<TypeSignature> argumentTypes = new ArrayList<>();

        private Parser(ScalarImplementationHeader header, Method method, Optional<Constructor<?>> constructor) {
            this.header = requireNonNull(header, "header is null");
            this.nullable = method.getAnnotation(SqlNullable.class) != null;
        }

        static ParametricScalarImplementation parseImplementation(ScalarImplementationHeader header, Method method, Optional<Constructor<?>> constructor)
        {
            return new Parser(header, method, constructor).get();
        }
    }
}