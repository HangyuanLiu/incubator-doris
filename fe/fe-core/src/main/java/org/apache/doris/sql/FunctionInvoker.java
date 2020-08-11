package org.apache.doris.sql;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMultimap;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.ExpressionFunctions;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.function.ScalarOperator;
import org.apache.doris.sql.function.SqlType;
import org.apache.doris.sql.type.BigintOperators;
import org.apache.doris.sql.type.OperatorType;
import org.apache.doris.sql.type.TypeSignature;
import org.apache.doris.sql.type.VarcharOperators;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.lang.String.format;

public enum FunctionInvoker {
    INSTANCE;

    private ImmutableMultimap<String, FEFunctionInvoker> functions;

    FunctionInvoker() {
        registerFunctions();
    }

    public Object invoke(Signature signature, List<Object> arg) throws AnalysisException{

        if (signature.getName().startsWith("cast")) {
            Signature newSig = new Signature(OperatorType.CAST.name(), signature.getArgTypes(), signature.getReturnType());
            FEFunctionInvoker invoker = getFunction(newSig);
            return invoker.invoke(arg);
        }
        FEFunctionInvoker invoker = getFunction(signature);
        if (invoker == null) {
            throw new AnalysisException(signature.getName() + " is not implement");
        }
        return invoker.invoke(arg);
    }

    public FEFunctionInvoker getFunction(Signature signature) {
        Collection<FEFunctionInvoker> functionInvokers = functions.get(signature.getName().toUpperCase());
        if (functionInvokers == null) {
            return null;
        }

        if (signature.getName().equalsIgnoreCase(OperatorType.CAST.name())) {
            for (FEFunctionInvoker invoker : functionInvokers) {
                if (signature.getReturnType().equals(invoker.getSignature().getReturnType())) {
                    return invoker;
                }
            }
        }

        for (FEFunctionInvoker invoker : functionInvokers) {
            List<TypeSignature> argTypes1 = invoker.getSignature().getArgTypes();
            List<TypeSignature> argTypes2 = signature.getArgTypes();

            if (!argTypes1.equals(argTypes2)) {
                continue;
            }
            return invoker;
        }
        return null;
    }

    private synchronized void registerFunctions() {
        // double checked locking pattern
        // functions only need to init once
        if (functions != null) {
            return;
        }

        ImmutableMultimap.Builder<String, FEFunctionInvoker> mapBuilder =
                new ImmutableMultimap.Builder<String, FEFunctionInvoker>();
        List<Class> operators = new ArrayList<>();
        operators.add(BigintOperators.class);
        operators.add(VarcharOperators.class);
        for (Class clazz : operators) {
            for (Method method : clazz.getDeclaredMethods()) {
                ScalarOperator annotation = method.getAnnotation(ScalarOperator.class);
                if (annotation != null) {
                    String name = annotation.value().name();

                    TypeSignature returnType = new TypeSignature(method.getAnnotation(SqlType.class).value());

                    List<TypeSignature> argumentTypes = new ArrayList<>();
                    int i = 0;
                    while (i < method.getParameterCount()) {
                        Parameter parameter = method.getParameters()[i];
                        Class<?> parameterType = parameter.getType();

                        Annotation[] annotations = parameter.getAnnotations();

                        SqlType type = Stream.of(annotations)
                                .filter(SqlType.class::isInstance)
                                .map(SqlType.class::cast)
                                .findFirst()
                                .orElseThrow(() -> new IllegalArgumentException(format("Method [%s] is missing @SqlType annotation for parameter", method)));
                        TypeSignature typeSignature = new TypeSignature(type.value());
                        argumentTypes.add(typeSignature);

                        Signature signature = new Signature(name, argumentTypes, returnType);
                        mapBuilder.put(name.toUpperCase(), new FEFunctionInvoker(method, signature));
                        ++i;
                    }
                }
            }
            this.functions = mapBuilder.build();
        }
    }

    public static class FEFunctionInvoker {
        private final Method method;
        private final Signature signature;

        public FEFunctionInvoker(Method method, Signature signature) {
            this.method = method;
            this.signature = signature;
        }

        public Method getMethod() {
            return method;
        }

        public Signature getSignature() {
            return signature;
        }

        public Object invoke(List<Object> args) throws AnalysisException {
            try {
                return method.invoke(null, args.toArray());
            } catch (InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
                throw new AnalysisException(e.getLocalizedMessage());
            }
        }
    }

    public static class Signature {
        private final String name;
        private final List<TypeSignature> argTypes;
        private final TypeSignature returnType;

        public Signature(String name, List<TypeSignature> argTypes, TypeSignature returnType) {
            this.name = name;
            this.argTypes = argTypes;
            this.returnType = returnType;
        }

        public List<TypeSignature> getArgTypes() {
            return argTypes;
        }

        public TypeSignature getReturnType() {
            return returnType;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("FEFunctionSignature. name: ").append(name).append(", return: ").append(returnType);
            sb.append(", args: ").append(Joiner.on(",").join(argTypes));
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Signature signature = (Signature) o;
            return Objects.equals(name, signature.name) && argTypes.equals(signature.argTypes)
                    && Objects.equals(returnType, signature.returnType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, argTypes, returnType);
        }
    }
}
