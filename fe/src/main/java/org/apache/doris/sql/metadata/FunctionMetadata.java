package org.apache.doris.sql.metadata;

import org.apache.doris.sql.type.TypeSignature;

import java.util.List;
import java.util.Optional;

public class FunctionMetadata {
    private final List<TypeSignature> argumentTypes;
    private final Optional<List<String>> argumentNames;
    private final TypeSignature returnType;

    public FunctionMetadata(List<TypeSignature> argumentTypes, Optional<List<String>> argumentNames, TypeSignature returnType) {
        this.argumentTypes = argumentTypes;
        this.argumentNames = argumentNames;
        this.returnType = returnType;
    }
}