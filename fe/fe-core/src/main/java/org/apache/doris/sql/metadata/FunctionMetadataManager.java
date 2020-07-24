package org.apache.doris.sql.metadata;

public interface FunctionMetadataManager
{
    FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle);
}