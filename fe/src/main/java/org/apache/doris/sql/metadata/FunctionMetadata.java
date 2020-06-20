package org.apache.doris.sql.metadata;

import java.util.List;
import java.util.Optional;

public class FunctionMetadata {
    private final QualifiedFunctionName name;
    private final Optional<OperatorType> operatorType;
    private final List<TypeSignature> argumentTypes;
    private final Optional<List<String>> argumentNames;
    private final TypeSignature returnType;
    private final FunctionKind functionKind;
}