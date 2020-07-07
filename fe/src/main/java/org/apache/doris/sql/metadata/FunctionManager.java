package org.apache.doris.sql.metadata;

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.sql.analyzer.TypeSignatureProvider;
import org.apache.doris.sql.tree.QualifiedName;
import org.apache.doris.sql.type.OperatorType;
import org.apache.doris.sql.type.Type;
import org.apache.doris.sql.type.TypeSignature;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;

@ThreadSafe
public class FunctionManager
        implements FunctionMetadataManager
{
    Catalog catalog;
    public FunctionManager(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle) {
        BuiltInFunctionHandle builtInFunctionHandle = (BuiltInFunctionHandle) functionHandle;

        Function searchDesc = new Function(new FunctionName(builtInFunctionHandle.getFunctionName()), builtInFunctionHandle.getArgumentTypes(), org.apache.doris.catalog.Type.INVALID, false);
        Function fn =  Catalog.getCurrentCatalog().getFunction(searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

        FunctionMetadata.FunctionKind fnKind;
        if (fn instanceof AggregateFunction) {
            fnKind = FunctionMetadata.FunctionKind.AGGREGATE;
        } else if (fn instanceof ScalarFunction) {
            fnKind = FunctionMetadata.FunctionKind.SCALAR;
        } else {
            throw new NotImplementedException();
        }


        List<TypeSignature> arguments = new ArrayList<>();
        for (org.apache.doris.catalog.Type type : fn.getArgs()) {
            arguments.add(new TypeSignature(type.toString()));
        }

        QualifiedFunctionName functionName =
                QualifiedFunctionName.of(new CatalogSchemaName("", ""), fn.getFunctionName().getFunction());

        return new FunctionMetadata(functionName, arguments, new TypeSignature(fn.getReturnType().toString()), fnKind);
    }

    public FunctionHandle resolveOperator(OperatorType operatorType,  List<TypeSignatureProvider> argumentTypes) {
        List<TypeSignature> arguments = new ArrayList<>();
        for(TypeSignatureProvider type : argumentTypes) {
            arguments.add(type.getTypeSignature());
        }
        if (operatorType.equals(OperatorType.EQUAL)) {
            return new BuiltInFunctionHandle(null, "eq", arguments);
        } else if (operatorType.equals(OperatorType.ADD)) {
            return new BuiltInFunctionHandle(null, "add", arguments);
        }
        else {
            throw new UnsupportedOperationException("not yet implemented");
        }
    }

    public FunctionHandle resolveFunction(QualifiedName functionName, List<TypeSignatureProvider> parameterTypes)
    {
        List<TypeSignature> arguments = new ArrayList<>();
        for (TypeSignatureProvider type : parameterTypes) {
            arguments.add(type.getTypeSignature());
        }
        return new BuiltInFunctionHandle(null, functionName.toString(), arguments);
    }
}