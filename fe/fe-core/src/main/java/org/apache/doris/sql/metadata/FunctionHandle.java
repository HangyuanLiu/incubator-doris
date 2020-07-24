package org.apache.doris.sql.metadata;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Type;
import org.apache.doris.sql.type.TypeSignature;

import java.util.ArrayList;
import java.util.List;

public class FunctionHandle {
    public enum FunctionKind
    {
        SCALAR,
        AGGREGATE,
        WINDOW
    }

    private String functionName;
    private final TypeSignature returnType;
    private final TypeSignature interminateTypes;
    private final List<TypeSignature> argumentTypes;
    private final FunctionKind functionKind;
    private final Function fn;

    public FunctionHandle(
            String functionName,
            TypeSignature returnType,
            TypeSignature interminateTypes,
            List<TypeSignature> argumentTypes,
            FunctionKind functionKind,
            Function fn) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.interminateTypes = interminateTypes;
        this.argumentTypes = argumentTypes;
        this.functionKind = functionKind;
        this.fn = fn;
    }

    public String getFunctionName() {
        return functionName;
    }

    public List<TypeSignature> getArgumentTypes() {
        return argumentTypes;
    }

    public TypeSignature getInterminateTypes() {
        return interminateTypes;
    }

    public TypeSignature getReturnType() {
        return returnType;
    }

    public Function getResolevedFn() {
        return fn;
    }

    public FunctionKind getFunctionKind() {
        return functionKind;
    }
}
