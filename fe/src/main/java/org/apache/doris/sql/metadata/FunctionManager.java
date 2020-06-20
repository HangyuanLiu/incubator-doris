package org.apache.doris.sql.metadata;

import javax.annotation.concurrent.ThreadSafe;

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
}