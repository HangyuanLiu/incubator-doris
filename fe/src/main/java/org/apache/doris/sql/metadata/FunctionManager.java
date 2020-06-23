package org.apache.doris.sql.metadata;

import org.apache.doris.sql.analyzer.TypeSignatureProvider;
import org.apache.doris.sql.type.OperatorType;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

@ThreadSafe
public class FunctionManager
        implements FunctionMetadataManager
{
    public FunctionManager() {

    }

    @Override
    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle) {
        /*
        Optional<FunctionNamespaceManager<?>> functionNamespaceManager = getServingFunctionNamespaceManager(functionHandle.getFunctionNamespace());
        checkArgument(functionNamespaceManager.isPresent(), "Cannot find function namespace for '%s'", functionHandle.getFunctionNamespace());
        return functionNamespaceManager.get().getFunctionMetadata(functionHandle);
         */
        return null;
    }

    public FunctionHandle resolveOperator(OperatorType operatorType, List<TypeSignatureProvider> argumentTypes) {
        return null;
    }
}