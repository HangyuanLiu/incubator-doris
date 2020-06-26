package org.apache.doris.sql.metadata;

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Function;
import org.apache.doris.sql.type.OperatorType;
import org.apache.doris.sql.type.Type;
import org.apache.doris.sql.type.TypeSignature;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;

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

        List<TypeSignature> arguments = new ArrayList<>();
        for (org.apache.doris.catalog.Type type : fn.getArgs()) {
            arguments.add(new TypeSignature(type.toString()));
        }

        return new FunctionMetadata(arguments, new TypeSignature(fn.getReturnType().toString()));
    }

    public FunctionHandle resolveOperator(OperatorType operatorType, List<Type> argumentTypes) {
        List<TypeSignature> arguments = new ArrayList<>();
        for (Type type : argumentTypes) {
            arguments.add(type.getTypeSignature());
        }
        return new BuiltInFunctionHandle(null, "eq", arguments);
    }
}