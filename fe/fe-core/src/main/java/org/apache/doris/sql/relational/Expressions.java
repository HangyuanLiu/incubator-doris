/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.doris.sql.relational;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.sql.analyzer.TypeSignatureProvider;
import org.apache.doris.sql.metadata.FunctionHandle;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.ConstantExpression;
import org.apache.doris.sql.relation.InputReferenceExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.RowExpressionVisitor;
import org.apache.doris.sql.relation.SpecialFormExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.type.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Set;



public final class Expressions
{
    private Expressions()
    {
    }

    public static ConstantExpression constant(Object value, Type type)
    {
        return new ConstantExpression(value, type);
    }

    public static ConstantExpression constantNull(Type type)
    {
        return new ConstantExpression(null, type);
    }

    public static boolean isNull(RowExpression expression)
    {
        return expression instanceof ConstantExpression && ((ConstantExpression) expression).isNull();
    }

    public static CallExpression call(String displayName, FunctionHandle functionHandle, Type returnType, RowExpression... arguments)
    {
        return new CallExpression(displayName, functionHandle, returnType, Arrays.asList(arguments));
    }

    public static CallExpression call(String displayName, FunctionHandle functionHandle, Type returnType, List<RowExpression> arguments)
    {
        return new CallExpression(displayName, functionHandle, returnType, arguments);
    }
    /*
    public static CallExpression call(FunctionManager functionManager, String name, Type returnType, RowExpression... arguments)
    {
        return call(functionManager, name, returnType, ImmutableList.copyOf(arguments));
    }

    public static CallExpression call(FunctionManager functionManager, String name, Type returnType, List<RowExpression> arguments)
    {
        FunctionHandle functionHandle = functionManager.lookupFunction(name, TypeSignatureProvider.fromTypes(arguments.stream().map(RowExpression::getType).collect(Collectors.toList())));
        return call(name, functionHandle, returnType, arguments);
    }

     */

    public static InputReferenceExpression field(int field, Type type)
    {
        return new InputReferenceExpression(field, type);
    }

    public static SpecialFormExpression specialForm(SpecialFormExpression.Form form, Type returnType, RowExpression... arguments)
    {
        return new SpecialFormExpression(form, returnType, arguments);
    }

    public static SpecialFormExpression specialForm(SpecialFormExpression.Form form, Type returnType, List<RowExpression> arguments)
    {
        return new SpecialFormExpression(form, returnType, arguments);
    }

    public static Set<RowExpression> uniqueSubExpressions(RowExpression expression)
    {
        return ImmutableSet.copyOf(subExpressions(ImmutableList.of(expression)));
    }

    public static List<RowExpression> subExpressions(RowExpression expression)
    {
        return subExpressions(ImmutableList.of(expression));
    }

    public static List<RowExpression> subExpressions(Iterable<RowExpression> expressions)
    {
        final ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();

        for (RowExpression expression : expressions) {
            expression.accept(new RowExpressionVisitor<Void, Void>()
            {
                @Override
                public Void visitCall(CallExpression call, Void context)
                {
                    builder.add(call);
                    for (RowExpression argument : call.getArguments()) {
                        argument.accept(this, context);
                    }
                    return null;
                }

                @Override
                public Void visitInputReference(InputReferenceExpression reference, Void context)
                {
                    builder.add(reference);
                    return null;
                }

                @Override
                public Void visitConstant(ConstantExpression literal, Void context)
                {
                    builder.add(literal);
                    return null;
                }

                @Override
                public Void visitVariableReference(VariableReferenceExpression reference, Void context)
                {
                    builder.add(reference);
                    return null;
                }

                @Override
                public Void visitSpecialForm(SpecialFormExpression specialForm, Void context)
                {
                    builder.add(specialForm);
                    for (RowExpression argument : specialForm.getArguments()) {
                        argument.accept(this, context);
                    }
                    return null;
                }
            }, null);
        }

        return builder.build();
    }

    public static List<RowExpression> extractAnd(RowExpression expression)
    {
        final ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
        expression.accept(new RowExpressionVisitor<Void, Void>() {
            @Override
            public Void visitCall(CallExpression call, Void context) {
                builder.add(call);
                return null;
            }

            @Override
            public Void visitInputReference(InputReferenceExpression reference, Void context) {
                builder.add(reference);
                return null;
            }

            @Override
            public Void visitConstant(ConstantExpression literal, Void context) {
                builder.add(literal);
                return null;
            }

            @Override
            public Void visitVariableReference(VariableReferenceExpression reference, Void context) {
                builder.add(reference);
                return null;
            }

            @Override
            public Void visitSpecialForm(SpecialFormExpression specialForm, Void context) {
                if (specialForm.getForm().equals(SpecialFormExpression.Form.AND)) {
                    for (RowExpression argument : specialForm.getArguments()) {
                        argument.accept(this, context);
                    }
                } else {
                    builder.add(specialForm);
                }
                return null;
            }
        }, null);

        return builder.build();
    }

    public static VariableReferenceExpression variable(String name, Type type)
    {
        return new VariableReferenceExpression(name, type);
    }
}
