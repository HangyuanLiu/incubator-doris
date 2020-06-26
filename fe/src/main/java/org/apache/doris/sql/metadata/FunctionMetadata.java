package org.apache.doris.sql.metadata;

import org.apache.doris.sql.type.TypeSignature;

import java.util.List;
import java.util.Optional;

public class FunctionMetadata {
    private final List<TypeSignature> argumentTypes;
    private final TypeSignature returnType;

    public FunctionMetadata(List<TypeSignature> argumentTypes, TypeSignature returnType) {
        this.argumentTypes = argumentTypes;
        this.returnType = returnType;
    }

    public TypeSignature getReturnType()
    {
        return returnType;
    }

    public List<TypeSignature> getArgumentTypes() {
        return argumentTypes;
    }
}