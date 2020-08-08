package org.apache.doris.sql.function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ScalarFromAnnotationsParser {
    private ScalarFromAnnotationsParser() {
    }

    public static List<SqlFunction> parseFunctionDefinition(Class<?> clazz) {
        ImmutableList.Builder<SqlFunction> builder = ImmutableList.builder();
        for (ScalarHeaderAndMethods scalar : findScalarsInFunctionDefinitionClass(clazz)) {
            builder.add(parseParametricScalar(scalar, FunctionsParserHelper.findConstructor(clazz)));
        }
        return builder.build();
    }

    public static List<SqlFunction> parseFunctionDefinitions(Class<?> clazz) {
        ImmutableList.Builder<SqlFunction> builder = ImmutableList.builder();
        for (ScalarHeaderAndMethods methods : findScalarsInFunctionSetClass(clazz)) {
            // Non-static function only makes sense in classes annotated @ScalarFunction.
            builder.add(parseParametricScalar(methods, Optional.empty()));
        }
        return builder.build();
    }

    private static List<ScalarHeaderAndMethods> findScalarsInFunctionDefinitionClass(Class<?> annotated)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        List<ScalarImplementationHeader> classHeaders = ScalarImplementationHeader.fromAnnotatedElement(annotated);
        checkArgument(!classHeaders.isEmpty(), "Class [%s] that defines function must be annotated with @ScalarFunction or @ScalarOperator", annotated.getName());

        for (ScalarImplementationHeader header : classHeaders) {
            Set<Method> methods = FunctionsParserHelper.findPublicMethods(annotated, SqlType.class, ScalarFunction.class, ScalarOperator.class);
            //checkCondition(!methods.isEmpty(), FUNCTION_IMPLEMENTATION_ERROR, "Parametric class [%s] does not have any annotated methods", annotated.getName());
            for (Method method : methods) {
                checkArgument(method.getAnnotation(ScalarFunction.class) == null, "Parametric class method [%s] is annotated with @ScalarFunction", method);
                checkArgument(method.getAnnotation(ScalarOperator.class) == null, "Parametric class method [%s] is annotated with @ScalarOperator", method);
            }
            builder.add(new ScalarHeaderAndMethods(header, methods));
        }

        return builder.build();
    }

    private static List<ScalarHeaderAndMethods> findScalarsInFunctionSetClass(Class<?> annotated)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        for (Method method : FunctionsParserHelper.findPublicMethods(
                annotated,
                ImmutableSet.of(SqlType.class, ScalarFunction.class, ScalarOperator.class),
                ImmutableSet.of(SqlInvokedScalarFunction.class))) {
            checkCondition((method.getAnnotation(ScalarFunction.class) != null) || (method.getAnnotation(ScalarOperator.class) != null),
                    FUNCTION_IMPLEMENTATION_ERROR, "Method [%s] annotated with @SqlType is missing @ScalarFunction or @ScalarOperator", method);
            for (ScalarImplementationHeader header : ScalarImplementationHeader.fromAnnotatedElement(method)) {
                builder.add(new ScalarHeaderAndMethods(header, ImmutableSet.of(method)));
            }
        }
        return builder.build();
    }

    private static SqlFunction parseParametricScalar(ScalarHeaderAndMethods scalar, Optional<Constructor<?>> constructor)
    {
        ScalarImplementationHeader header = scalar.getHeader();

        Map<ParametricScalarImplementation.SpecializedSignature, ParametricScalarImplementation.Builder> signatures = new HashMap<>();
        for (Method method : scalar.getMethods()) {
            ParametricScalarImplementation implementation = ParametricScalarImplementation.Parser.parseImplementation(header, method, constructor);
            if (!signatures.containsKey(implementation.getSpecializedSignature())) {
                ParametricScalarImplementation.Builder builder = new ParametricScalarImplementation.Builder(
                        implementation.getSignature(),
                        implementation.getArgumentNativeContainerTypes(),
                        implementation.getSpecializedTypeParameters(),
                        implementation.getReturnNativeContainerType());
                signatures.put(implementation.getSpecializedSignature(), builder);
                builder.addChoices(implementation);
            }
            else {
                ParametricScalarImplementation.Builder builder = signatures.get(implementation.getSpecializedSignature());
                builder.addChoices(implementation);
            }
        }

        ParametricImplementationsGroup.Builder<ParametricScalarImplementation> implementationsBuilder = ParametricImplementationsGroup.builder();
        for (ParametricScalarImplementation.Builder implementation : signatures.values()) {
            implementationsBuilder.addImplementation(implementation.build());
        }
        ParametricImplementationsGroup<ParametricScalarImplementation> implementations = implementationsBuilder.build();
        Signature scalarSignature = implementations.getSignature();

        header.getOperatorType().ifPresent(operatorType ->
                validateOperator(operatorType, scalarSignature.getReturnType(), scalarSignature.getArgumentTypes()));

        return new ParametricScalar(scalarSignature, header.getHeader(), implementations);
    }

    private static class ScalarHeaderAndMethods
    {
        private final ScalarImplementationHeader header;
        private final Set<Method> methods;

        public ScalarHeaderAndMethods(ScalarImplementationHeader header, Set<Method> methods)
        {
            this.header = requireNonNull(header);
            this.methods = requireNonNull(methods);
        }

        public ScalarImplementationHeader getHeader()
        {
            return header;
        }

        public Set<Method> getMethods()
        {
            return methods;
        }
    }
}