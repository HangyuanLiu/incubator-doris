package org.apache.doris.sql.function;

import java.util.ArrayList;
import java.util.List;

public class FunctionListBuilder {
    private final List<SqlFunction> functions = new ArrayList<>();

    public FunctionListBuilder scalar(Class<?> clazz)
    {
        functions.addAll(ScalarFromAnnotationsParser.parseFunctionDefinition(clazz));
        return this;
    }

    public FunctionListBuilder scalars(Class<?> clazz)
    {
        functions.addAll(ScalarFromAnnotationsParser.parseFunctionDefinitions(clazz));
        return this;
    }
}