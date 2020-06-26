package org.apache.doris.sql.metadata;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.sql.type.TypeSignature;

import java.util.ArrayList;
import java.util.List;

public class BuiltInFunctionHandle implements FunctionHandle {
    private String db;
    private String functionName;
    private List<Type> argumentTypes;

    public BuiltInFunctionHandle(String db, String functionName, List<TypeSignature> arg) {
        this.db = db;
        this.functionName = functionName;
        this.argumentTypes = new ArrayList<>();

        for (TypeSignature type : arg) {
            if ("BIGINT".equals(type.getBase())) {
                this.argumentTypes.add(Type.BIGINT);
            }
        }
    }

    public String getFunctionName() {
        return functionName;
    }

    public List<Type> getArgumentTypes() {
        return argumentTypes;
    }

    @Override
    public CatalogSchemaName getFunctionNamespace() {
        return null;
    }
}
