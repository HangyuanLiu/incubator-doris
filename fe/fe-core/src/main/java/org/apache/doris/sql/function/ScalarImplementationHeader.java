package org.apache.doris.sql.function;

import org.apache.doris.sql.metadata.QualifiedFunctionName;
import org.apache.doris.sql.type.OperatorType;

import java.util.Optional;

public class ScalarImplementationHeader {
    private final QualifiedFunctionName name;
    private final Optional<OperatorType> operatorType;
    private final ScalarHeader header;
}