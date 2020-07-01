package org.apache.doris.sql.metadata;

import org.apache.doris.sql.type.TypeSignature;

import java.util.List;
import java.util.Optional;

public class FunctionMetadata {
    public enum FunctionKind
    {
        SCALAR,
        AGGREGATE,
        WINDOW
    }

    private final List<TypeSignature> argumentTypes;
    private final TypeSignature returnType;
    private final FunctionKind functionKind;

    public FunctionMetadata(List<TypeSignature> argumentTypes, TypeSignature returnType, FunctionKind functionKind) {
        this.argumentTypes = argumentTypes;
        this.returnType = returnType;
        this.functionKind = functionKind;
    }

    public TypeSignature getReturnType()
    {
        return returnType;
    }

    public List<TypeSignature> getArgumentTypes() {
        return argumentTypes;
    }

    public FunctionKind getFunctionKind()
    {
        return functionKind;
    }
}