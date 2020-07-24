package org.apache.doris.sql.metadata;

import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.sql.type.OperatorType;
import org.apache.doris.sql.type.TypeSignature;

import java.util.List;
import java.util.Optional;

public class FunctionMetadata {

    private final QualifiedFunctionName name;
    private Optional<OperatorType> operatorType;
    private final List<TypeSignature> argumentTypes;
    private final TypeSignature returnType;
    private final FunctionHandle.FunctionKind functionKind;

    public FunctionMetadata(
            QualifiedFunctionName name,
            List<TypeSignature> argumentTypes,
            TypeSignature returnType,
            FunctionHandle.FunctionKind functionKind,
            OperatorType operatorType) {
        this.name = name;
        this.argumentTypes = argumentTypes;
        this.returnType = returnType;
        this.functionKind = functionKind;
        if (operatorType == null) {
            this.operatorType = Optional.empty();
        } else {
            this.operatorType = Optional.of(operatorType);
        }
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

    public Optional<OperatorType> getOperatorType()
    {
        return operatorType;
    }

    public boolean isDeterministic()
    {
        return true;
    }
}