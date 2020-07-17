package org.apache.doris.sql.metadata;

import org.apache.doris.sql.type.TypeSignature;

import java.util.List;

public class FunctionMetadata {

    private final QualifiedFunctionName name;
    private final List<TypeSignature> argumentTypes;
    private final TypeSignature returnType;
    private final FunctionHandle.FunctionKind functionKind;



    public FunctionMetadata(
            QualifiedFunctionName name,
            List<TypeSignature> argumentTypes,
            TypeSignature returnType,
            FunctionHandle.FunctionKind functionKind) {
        this.name = name;
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

    public FunctionHandle.FunctionKind getFunctionKind()
    {
        return functionKind;
    }

    public QualifiedFunctionName getName()
    {
        return name;
    }
}