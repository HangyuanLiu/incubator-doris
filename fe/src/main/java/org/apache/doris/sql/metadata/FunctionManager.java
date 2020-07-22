package org.apache.doris.sql.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.sql.analyzer.TypeSignatureProvider;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.tree.ArithmeticBinaryExpression;
import org.apache.doris.sql.tree.QualifiedName;
import org.apache.doris.sql.type.BooleanType;
import org.apache.doris.sql.type.OperatorType;
import org.apache.doris.sql.type.StandardTypes;
import org.apache.doris.sql.type.Type;
import org.apache.doris.sql.type.TypeManager;
import org.apache.doris.sql.type.TypeSignature;
import org.apache.doris.sql.type.TypeSignatureParameter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@ThreadSafe
public class FunctionManager
        implements FunctionMetadataManager
{
    private final TypeManager typeManager;
    private final Catalog catalog;
    public FunctionManager(TypeManager typeManager, Catalog catalog) {
        this.typeManager = typeManager;
        this.catalog = catalog;
    }

    @Override
    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle) {
        //TODO: 为了兼容逻辑，这里应该是可以删掉的
        return new FunctionMetadata(
                new QualifiedFunctionName(null, functionHandle.getFunctionName()),
                functionHandle.getArgumentTypes(),
                functionHandle.getReturnType(),
                functionHandle.getFunctionKind());
    }

    public FunctionHandle resolveOperator(OperatorType operatorType,  List<TypeSignatureProvider> argumentTypes) {
        List<TypeSignature> arguments = argumentTypes.stream().map(TypeSignatureProvider::getTypeSignature).collect(Collectors.toList());

        String functionName = "";
        boolean isPredicate = false;
        switch (operatorType) {
            case ADD:
                functionName = "add"; break;
            case SUBTRACT:
                functionName = "subtract"; break;
            case MULTIPLY:
                functionName = "multiply"; break;
            case DIVIDE:
                functionName = "divide"; break;
            case MODULUS:
                functionName = "mod"; break;

            case EQUAL:
                functionName = "eq";
                isPredicate = true;
                break;
            case GREATER_THAN:
                functionName = "gt";
                isPredicate = true;
                break;
            case GREATER_THAN_OR_EQUAL:
                functionName = "ge";
                isPredicate = true;
                break;
            case LESS_THAN:
                functionName = "lt";
                isPredicate = true;
                break;
            case LESS_THAN_OR_EQUAL:
                functionName = "le";
                isPredicate = true;
                break;
            case NOT_EQUAL:
                functionName = "ne";
                isPredicate = true;
                break;
            default:
                throw new UnsupportedOperationException("not yet implemented");
        }
        if (arguments.stream().map(TypeSignature::getBase).anyMatch(type -> type.equals(StandardTypes.INTERVAL_DAY_TO_SECOND))) {
            return resolveTimeUnitOperator(operatorType, argumentTypes, "DAY");
        }

        Optional<Type> retType = Optional.of(typeManager.getType(argumentTypes.get(0).getTypeSignature()));
        for (TypeSignatureProvider type : argumentTypes) {
            retType = typeManager.getCommonSuperType(typeManager.getType(type.getTypeSignature()), retType.get());
        }

        List<TypeSignature> coerceTypes = new ArrayList<>();
        for (TypeSignature argType : arguments) {
            coerceTypes.add(typeManager.coerceTypeBase(typeManager.getType(argType), retType.get().getTypeSignature().getBase()).get().getTypeSignature());
        }

        if (isPredicate) {
            retType = Optional.of(BooleanType.BOOLEAN);
        }

        return new FunctionHandle(functionName,
                retType.get().getTypeSignature(),
                null,
                coerceTypes,
                FunctionHandle.FunctionKind.SCALAR, null);
    }

    public FunctionHandle resolveTimeUnitOperator(OperatorType operatorType, List<TypeSignatureProvider> argumentTypes, String timeUnit) {
        List<TypeSignature> arguments = argumentTypes.stream().map(TypeSignatureProvider::getTypeSignature).collect(Collectors.toList());
        String funcOpName = "";

        switch (operatorType) {
            case ADD:
                funcOpName = String.format("%sS_%s", timeUnit, "ADD");
                break;
            case SUBTRACT:
                funcOpName = String.format("%sS_%s", timeUnit, "SUB");
                break;
        }

        Function searchDesc = new Function(
                new FunctionName(funcOpName),
                arguments.stream().map(TypeSignature::toDorisType).collect(Collectors.toList()),
                org.apache.doris.catalog.Type.INVALID, false);
        Function fn = Catalog.getCurrentCatalog().getFunction(searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

        return new FunctionHandle(funcOpName,
                TypeSignature.create(fn.getReturnType()),
                null,
                arguments, FunctionHandle.FunctionKind.SCALAR, fn);
    }

    public FunctionHandle resolveFunction(QualifiedName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        List<TypeSignature> arguments = parameterTypes.stream().map(TypeSignatureProvider::getTypeSignature).collect(Collectors.toList());

        Function searchDesc = new Function(
                new FunctionName(functionName.toString()),
                arguments.stream().map(TypeSignature::toDorisType).collect(Collectors.toList()),
                org.apache.doris.catalog.Type.INVALID, false);
        Function fn = Catalog.getCurrentCatalog().getFunction(searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

        FunctionHandle.FunctionKind functionKind;
        TypeSignature intermediateType;
        if (fn instanceof AggregateFunction) {
            AggregateFunction fnAgg = (AggregateFunction) fn;
            functionKind = FunctionHandle.FunctionKind.AGGREGATE;
            intermediateType = TypeSignature.create(
                    (fnAgg.getIntermediateType() == null ? fnAgg.getReturnType() : fnAgg.getIntermediateType()));
        } else {
            functionKind = FunctionHandle.FunctionKind.SCALAR;
            intermediateType = null;
        }

        return new FunctionHandle(functionName.toString(),
                TypeSignature.create(fn.getReturnType()),
                intermediateType,
                arguments, functionKind, fn);
    }

    public FunctionHandle likeFunction(List<TypeSignatureProvider> parameterTypes) {
        List<TypeSignature> arguments = parameterTypes.stream().map(TypeSignatureProvider::getTypeSignature).collect(Collectors.toList());
        return new FunctionHandle("LIKE", BooleanType.BOOLEAN.getTypeSignature(),
                null, arguments, FunctionHandle.FunctionKind.SCALAR, null);
    }

    public FunctionHandle lookupCast(TypeSignature fromType, TypeSignature toType)
    {
        Function searchDesc = new Function(new FunctionName("castTo" + toType.toDorisType().toString()),
                Lists.newArrayList(fromType.toDorisType()),
                org.apache.doris.catalog.Type.INVALID, false);

        Function fn = Catalog.getCurrentCatalog().getFunction(searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

        return new FunctionHandle("castTo" + toType.toDorisType().toString(),
                TypeSignature.create(fn.getReturnType()),
                null,
                Lists.newArrayList(fromType), FunctionHandle.FunctionKind.SCALAR, fn);
    }
}