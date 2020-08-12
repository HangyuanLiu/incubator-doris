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
package org.apache.doris.sql.planner;

import org.apache.doris.sql.InterpretedFunctionInvoker;
import org.apache.doris.sql.metadata.ConnectorSession;
import org.apache.doris.sql.metadata.FunctionHandle;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.metadata.FunctionMetadata;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.ConstantExpression;
import org.apache.doris.sql.relation.InputReferenceExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.RowExpressionVisitor;
import org.apache.doris.sql.relation.SpecialFormExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import org.apache.doris.sql.relational.FunctionResolution;
import org.apache.doris.sql.relational.RowExpressionDeterminismEvaluator;
import org.apache.doris.sql.type.OperatorType;
import org.apache.doris.sql.type.RowType;
import org.apache.doris.sql.type.Type;
import org.apache.doris.sql.type.TypeManager;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.doris.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.apache.doris.sql.planner.LiteralEncoder.isSupportedLiteralType;
import static org.apache.doris.sql.planner.RowExpressionInterpreter.SpecialCallResult.changed;
import static org.apache.doris.sql.planner.RowExpressionInterpreter.SpecialCallResult.notChanged;
import static org.apache.doris.sql.relation.ExpressionOptimizer.Level;
import static org.apache.doris.sql.relation.ExpressionOptimizer.Level.EVALUATED;
import static org.apache.doris.sql.relation.ExpressionOptimizer.Level.SERIALIZABLE;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.AND;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.BIND;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.COALESCE;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.DEREFERENCE;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.IF;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.IN;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.IS_NULL;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.NULL_IF;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.OR;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.SWITCH;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.WHEN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Verify.verify;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.doris.sql.relational.Expressions.call;
import static org.apache.doris.sql.type.BooleanType.BOOLEAN;
import static org.apache.doris.sql.type.OperatorType.CAST;
import static org.apache.doris.sql.type.OperatorType.EQUAL;

public class RowExpressionInterpreter
{
    private static final long MAX_SERIALIZABLE_OBJECT_SIZE = 1000;
    private final RowExpression expression;
    private final Metadata metadata;
    private final ConnectorSession session;
    private final Level optimizationLevel;
    private final InterpretedFunctionInvoker functionInvoker;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;
    private final FunctionManager functionManager;
    private final FunctionResolution resolution;

    private final Visitor visitor;

    public static Object evaluateConstantRowExpression(RowExpression expression, Metadata metadata, ConnectorSession session)
    {
        // evaluate the expression
        Object result = new RowExpressionInterpreter(expression, metadata, session, EVALUATED).evaluate();
        verify(!(result instanceof RowExpression), "RowExpression interpreter returned an unresolved expression");
        return result;
    }

    public static RowExpressionInterpreter rowExpressionInterpreter(RowExpression expression, Metadata metadata, ConnectorSession session)
    {
        return new RowExpressionInterpreter(expression, metadata, session, EVALUATED);
    }

    public RowExpressionInterpreter(RowExpression expression, Metadata metadata, ConnectorSession session, Level optimizationLevel)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.optimizationLevel = optimizationLevel;
        this.functionInvoker = new InterpretedFunctionInvoker(metadata.getFunctionManager());
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionManager());
        this.resolution = new FunctionResolution(metadata.getFunctionManager());
        this.functionManager = metadata.getFunctionManager();

        this.visitor = new Visitor();
    }

    public Type getType()
    {
        return expression.getType();
    }

    public Object evaluate()
    {
        checkState(optimizationLevel.ordinal() >= EVALUATED.ordinal(), "evaluate() not allowed for optimizer");
        return expression.accept(visitor, null);
    }

    public Object optimize()
    {
        checkState(optimizationLevel.ordinal() < EVALUATED.ordinal(), "optimize() not allowed for interpreter");
        return optimize(null);
    }

    public Object optimize(VariableResolver inputs)
    {
        checkState(optimizationLevel.ordinal() <= EVALUATED.ordinal(), "optimize(SymbolResolver) not allowed for interpreter");
        return expression.accept(visitor, inputs);
    }

    private class Visitor
            implements RowExpressionVisitor<Object, Object>
    {
        @Override
        public Object visitInputReference(InputReferenceExpression node, Object context)
        {
            return node;
        }

        @Override
        public Object visitConstant(ConstantExpression node, Object context)
        {
            return node.getValue();
        }

        @Override
        public Object visitVariableReference(VariableReferenceExpression node, Object context)
        {
            if (context instanceof VariableResolver) {
                return ((VariableResolver) context).getValue(node);
            }
            return node;
        }

        @Override
        public Object visitCall(CallExpression node, Object context)
        {
            if (node.getDisplayName().equalsIgnoreCase(CAST.name()) &&
                node.getArguments().get(0) instanceof SpecialFormExpression) {
                SpecialFormExpression expression = (SpecialFormExpression) node.getArguments().get(0);
                if (expression.getForm().equals(ROW_CONSTRUCTOR)) {
                    return expression.getArguments().get(0).toString();
                }
            }

            List<Type> argumentTypes = new ArrayList<>();
            List<Object> argumentValues = new ArrayList<>();
            for (RowExpression expression : node.getArguments()) {
                Object value = expression.accept(this, context);
                argumentValues.add(value);
                argumentTypes.add(expression.getType());
            }

            FunctionHandle functionHandle = node.getFunctionHandle();
            FunctionMetadata functionMetadata = metadata.getFunctionManager().getFunctionMetadata(node.getFunctionHandle());
            /* FIXME
            if (!functionMetadata.isCalledOnNullInput()) {
                for (Object value : argumentValues) {
                    if (value == null) {
                        return null;
                    }
                }
            }
            */
            // Special casing for cast
            if (resolution.isCastFunction(functionHandle)) {
                SpecialCallResult result = tryHandleCast(node, argumentValues);
                if (result.isChanged()) {
                    return result.getValue();
                }
            }

            // Special casing for like
            /*
            if (resolution.isLikeFunction(functionHandle)) {
                SpecialCallResult result = tryHandleLike(node, argumentValues, argumentTypes, context);
                if (result.isChanged()) {
                    return result.getValue();
                }
            }
            */
            if (functionMetadata.getFunctionKind() != FunctionHandle.FunctionKind.SCALAR) {
                return call(node.getDisplayName(), functionHandle, node.getType(), toRowExpressions(argumentValues, node.getArguments()));
            }

            // do not optimize non-deterministic functions
            if (optimizationLevel.ordinal() < EVALUATED.ordinal() &&
                    (!functionMetadata.isDeterministic() || hasUnresolvedValue(argumentValues) || resolution.isFailFunction(functionHandle))) {
                return call(node.getDisplayName(), functionHandle, node.getType(), toRowExpressions(argumentValues, node.getArguments()));
            }

            Object value;
            value = functionInvoker.invoke(functionHandle, argumentValues);
            //FIXME
            if (value == null) {
                return call(node.getDisplayName(), functionHandle, node.getType(), toRowExpressions(argumentValues, node.getArguments()));
            }

            if (optimizationLevel.ordinal() <= SERIALIZABLE.ordinal() && !isSerializable(value, node.getType())) {
                return call(node.getDisplayName(), functionHandle, node.getType(), toRowExpressions(argumentValues, node.getArguments()));
            }
            return value;
        }

        @Override
        public Object visitSpecialForm(SpecialFormExpression node, Object context) {
            switch (node.getForm()) {
                case IF: {
                    checkArgument(node.getArguments().size() == 3);
                    Object condition = processWithExceptionHandling(node.getArguments().get(0), context);

                    if (condition instanceof RowExpression) {
                        return new SpecialFormExpression(
                                IF,
                                node.getType(),
                                toRowExpression(condition, node.getArguments().get(0)),
                                toRowExpression(processWithExceptionHandling(node.getArguments().get(1), context), node.getArguments().get(1)),
                                toRowExpression(processWithExceptionHandling(node.getArguments().get(2), context), node.getArguments().get(2)));
                    }
                    else if (Boolean.TRUE.equals(condition)) {
                        return processWithExceptionHandling(node.getArguments().get(1), context);
                    }

                    return processWithExceptionHandling(node.getArguments().get(2), context);
                }
                case NULL_IF: {
                    checkArgument(node.getArguments().size() == 2);
                    Object left = processWithExceptionHandling(node.getArguments().get(0), context);
                    if (left == null) {
                        return null;
                    }

                    Object right = processWithExceptionHandling(node.getArguments().get(1), context);
                    if (right == null) {
                        return left;
                    }

                    if (hasUnresolvedValue(left, right)) {
                        return new SpecialFormExpression(
                                NULL_IF,
                                node.getType(),
                                toRowExpression(left, node.getArguments().get(0)),
                                toRowExpression(right, node.getArguments().get(1)));
                    }

                    Type leftType = node.getArguments().get(0).getType();
                    Type rightType = node.getArguments().get(1).getType();
                    Type commonType = metadata.getTypeManager().getCommonSuperType(leftType, rightType).get();
                    FunctionHandle firstCast = metadata.getFunctionManager().lookupCast(leftType.getTypeSignature(), commonType.getTypeSignature());
                    FunctionHandle secondCast = metadata.getFunctionManager().lookupCast(rightType.getTypeSignature(), commonType.getTypeSignature());

                    // cast(first as <common type>) == cast(second as <common type>)
                    boolean equal = Boolean.TRUE.equals(invokeOperator(
                            EQUAL,
                            ImmutableList.of(commonType, commonType),
                            ImmutableList.of(
                                    functionInvoker.invoke(firstCast, left),
                                    functionInvoker.invoke(secondCast, right))));

                    if (equal) {
                        return null;
                    }
                    return left;
                }
                case IS_NULL: {
                    checkArgument(node.getArguments().size() == 1);
                    Object value = processWithExceptionHandling(node.getArguments().get(0), context);
                    if (value instanceof RowExpression) {
                        return new SpecialFormExpression(
                                IS_NULL,
                                node.getType(),
                                toRowExpression(value, node.getArguments().get(0)));
                    }
                    return value == null;
                }
                case AND: {
                    Object left = node.getArguments().get(0).accept(this, context);
                    Object right;

                    if (Boolean.FALSE.equals(left)) {
                        return false;
                    }

                    right = node.getArguments().get(1).accept(this, context);

                    if (Boolean.TRUE.equals(right)) {
                        return left;
                    }

                    if (Boolean.FALSE.equals(right) || Boolean.TRUE.equals(left)) {
                        return right;
                    }

                    if (left == null && right == null) {
                        return null;
                    }
                    return new SpecialFormExpression(
                            AND,
                            node.getType(),
                            toRowExpressions(
                                    asList(left, right),
                                    node.getArguments().subList(0, 2)));
                }
                case OR: {
                    Object left = node.getArguments().get(0).accept(this, context);
                    Object right;

                    if (Boolean.TRUE.equals(left)) {
                        return true;
                    }

                    right = node.getArguments().get(1).accept(this, context);

                    if (Boolean.FALSE.equals(right)) {
                        return left;
                    }

                    if (Boolean.TRUE.equals(right) || Boolean.FALSE.equals(left)) {
                        return right;
                    }

                    if (left == null && right == null) {
                        return null;
                    }
                    return new SpecialFormExpression(
                            OR,
                            node.getType(),
                            toRowExpressions(
                                    asList(left, right),
                                    node.getArguments().subList(0, 2)));
                }
                case ROW_CONSTRUCTOR: {
                    RowType rowType = (RowType) node.getType();
                    List<Type> parameterTypes = rowType.getTypeParameters();
                    List<RowExpression> arguments = node.getArguments();
                    checkArgument(parameterTypes.size() == arguments.size(), "RowConstructor does not contain all fields");
                    for (int i = 0; i < parameterTypes.size(); i++) {
                        checkArgument(parameterTypes.get(i).equals(arguments.get(i).getType()), "RowConstructor has field with incorrect type");
                    }

                    int cardinality = arguments.size();
                    List<Object> values = new ArrayList<>(cardinality);
                    arguments.forEach(argument -> values.add(argument.accept(this, context)));
                    if (hasUnresolvedValue(values)) {
                        return new SpecialFormExpression(ROW_CONSTRUCTOR, node.getType(), toRowExpressions(values, node.getArguments()));
                    }
                    else {
                        //FIXME
                        return null;
                        /*
                        BlockBuilder blockBuilder = new RowBlockBuilder(parameterTypes, null, 1);
                        BlockBuilder singleRowBlockWriter = blockBuilder.beginBlockEntry();
                        for (int i = 0; i < cardinality; ++i) {
                            writeNativeValue(parameterTypes.get(i), singleRowBlockWriter, values.get(i));
                        }
                        blockBuilder.closeEntry();
                        return rowType.getObject(blockBuilder, 0);
                         */
                    }
                }
                case COALESCE: {
                    Type type = node.getType();
                    List<Object> values = node.getArguments().stream()
                            .map(value -> processWithExceptionHandling(value, context))
                            .filter(Objects::nonNull)
                            .flatMap(expression -> {
                                if (expression instanceof SpecialFormExpression && ((SpecialFormExpression) expression).getForm() == COALESCE) {
                                    return ((SpecialFormExpression) expression).getArguments().stream();
                                }
                                return Stream.of(expression);
                            })
                            .collect(toList());

                    if ((!values.isEmpty() && !(values.get(0) instanceof RowExpression)) || values.size() == 1) {
                        return values.get(0);
                    }
                    ImmutableList.Builder<RowExpression> operandsBuilder = ImmutableList.builder();
                    Set<RowExpression> visitedExpression = new HashSet<>();
                    for (Object value : values) {
                        RowExpression expression = LiteralEncoder.toRowExpression(value, type);
                        if (!determinismEvaluator.isDeterministic(expression) || visitedExpression.add(expression)) {
                            operandsBuilder.add(expression);
                        }
                        if (expression instanceof ConstantExpression && !(((ConstantExpression) expression).getValue() == null)) {
                            break;
                        }
                    }
                    List<RowExpression> expressions = operandsBuilder.build();

                    if (expressions.isEmpty()) {
                        return null;
                    }

                    if (expressions.size() == 1) {
                        return getOnlyElement(expressions);
                    }
                    return new SpecialFormExpression(COALESCE, node.getType(), expressions);
                }
                case IN: {
                    checkArgument(node.getArguments().size() >= 2, "values must not be empty");

                    // use toList to handle null values
                    List<RowExpression> valueExpressions = node.getArguments().subList(1, node.getArguments().size());
                    List<Object> values = valueExpressions.stream().map(value -> value.accept(this, context)).collect(toList());
                    List<Type> valuesTypes = valueExpressions.stream().map(RowExpression::getType).collect(toImmutableList());
                    Object target = node.getArguments().get(0).accept(this, context);
                    Type targetType = node.getArguments().get(0).getType();

                    if (target == null) {
                        return null;
                    }

                    boolean hasUnresolvedValue = false;
                    if (target instanceof RowExpression) {
                        hasUnresolvedValue = true;
                    }

                    boolean hasNullValue = false;
                    boolean found = false;
                    List<RowExpression> unresolvedValues = new ArrayList<>(values.size());
                    for (int i = 0; i < values.size(); i++) {
                        Object value = values.get(i);
                        Type valueType = valuesTypes.get(i);
                        if (value instanceof RowExpression || target instanceof RowExpression) {
                            hasUnresolvedValue = true;
                            unresolvedValues.add(toRowExpression(value, valueExpressions.get(i)));
                            continue;
                        }

                        if (value == null) {
                            hasNullValue = true;
                        }
                        else {
                            Boolean result = (Boolean) invokeOperator(EQUAL, ImmutableList.of(targetType, valueType), ImmutableList.of(target, value));
                            if (result == null) {
                                hasNullValue = true;
                            }
                            else if (!found && result) {
                                // in does not short-circuit so we must evaluate all value in the list
                                found = true;
                            }
                        }
                    }
                    if (found) {
                        return true;
                    }

                    if (hasUnresolvedValue) {
                        List<RowExpression> simplifiedExpressionValues = Stream.concat(
                                Stream.concat(
                                        Stream.of(toRowExpression(target, node.getArguments().get(0))),
                                        unresolvedValues.stream().filter(determinismEvaluator::isDeterministic).distinct()),
                                unresolvedValues.stream().filter((expression -> !determinismEvaluator.isDeterministic(expression))))
                                .collect(toImmutableList());
                        return new SpecialFormExpression(IN, node.getType(), simplifiedExpressionValues);
                    }
                    if (hasNullValue) {
                        return null;
                    }
                    return false;
                }
                case BIND: {
                    List<Object> values = node.getArguments()
                            .stream()
                            .map(value -> value.accept(this, context))
                            .collect(toImmutableList());
                    if (hasUnresolvedValue(values)) {
                        return new SpecialFormExpression(
                                BIND,
                                node.getType(),
                                toRowExpressions(values, node.getArguments()));
                    }
                    return insertArguments((MethodHandle) values.get(values.size() - 1), 0, values.subList(0, values.size() - 1).toArray());
                }
                case SWITCH: {
                    List<RowExpression> whenClauses;
                    Object elseValue = null;
                    RowExpression last = node.getArguments().get(node.getArguments().size() - 1);
                    if (last instanceof SpecialFormExpression && ((SpecialFormExpression) last).getForm().equals(WHEN)) {
                        whenClauses = node.getArguments().subList(1, node.getArguments().size());
                    }
                    else {
                        whenClauses = node.getArguments().subList(1, node.getArguments().size() - 1);
                    }

                    List<RowExpression> simplifiedWhenClauses = new ArrayList<>();
                    Object value = processWithExceptionHandling(node.getArguments().get(0), context);
                    if (value != null) {
                        for (RowExpression whenClause : whenClauses) {
                            checkArgument(whenClause instanceof SpecialFormExpression && ((SpecialFormExpression) whenClause).getForm().equals(WHEN));

                            RowExpression operand = ((SpecialFormExpression) whenClause).getArguments().get(0);
                            RowExpression result = ((SpecialFormExpression) whenClause).getArguments().get(1);

                            Object operandValue = processWithExceptionHandling(operand, context);

                            // call equals(value, operand)
                            if (operandValue instanceof RowExpression || value instanceof RowExpression) {
                                // cannot fully evaluate, add updated whenClause
                                simplifiedWhenClauses.add(new SpecialFormExpression(WHEN, whenClause.getType(), toRowExpression(operandValue, operand), toRowExpression(processWithExceptionHandling(result, context), result)));
                            }
                            else if (operandValue != null) {
                                Boolean isEqual = (Boolean) invokeOperator(
                                        EQUAL,
                                        ImmutableList.of(node.getArguments().get(0).getType(), operand.getType()),
                                        ImmutableList.of(value, operandValue));
                                if (isEqual != null && isEqual) {
                                    if (simplifiedWhenClauses.isEmpty()) {
                                        // this is the left-most true predicate. So return it.
                                        return processWithExceptionHandling(result, context);
                                    }

                                    elseValue = processWithExceptionHandling(result, context);
                                    break; // Done we found the last match. Don't need to go any further.
                                }
                            }
                        }
                    }

                    if (elseValue == null) {
                        elseValue = processWithExceptionHandling(last, context);
                    }

                    if (simplifiedWhenClauses.isEmpty()) {
                        return elseValue;
                    }

                    ImmutableList.Builder<RowExpression> argumentsBuilder = ImmutableList.builder();
                    argumentsBuilder.add(toRowExpression(value, node.getArguments().get(0)))
                            .addAll(simplifiedWhenClauses)
                            .add(toRowExpression(elseValue, last));
                    return new SpecialFormExpression(SWITCH, node.getType(), argumentsBuilder.build());
                }
                default:
                    throw new IllegalStateException("Can not compile special form: " + node.getForm());
            }
        }

        private Object processWithExceptionHandling(RowExpression expression, Object context)
        {
            if (expression == null) {
                return null;
            }
            try {
                return expression.accept(this, context);
            }
            catch (RuntimeException e) {
                // HACK
                // Certain operations like 0 / 0 or likeExpression may throw exceptions.
                // Wrap them in a call that will throw the exception if the expression is actually executed
                //return createFailureFunction(e, expression.getType());
                return null;
            }
        }

        private boolean hasUnresolvedValue(Object... values)
        {
            return hasUnresolvedValue(ImmutableList.copyOf(values));
        }

        private boolean hasUnresolvedValue(List<Object> values)
        {
            return values.stream().anyMatch(instanceOf(RowExpression.class)::apply);
        }

        private Object invokeOperator(OperatorType operatorType, List<? extends Type> argumentTypes, List<Object> argumentValues)
        {
            FunctionHandle operatorHandle = metadata.getFunctionManager().resolveOperator(operatorType, fromTypes(argumentTypes));
            return functionInvoker.invoke(operatorHandle, argumentValues);
        }

        private List<RowExpression> toRowExpressions(List<Object> values, List<RowExpression> unchangedValues)
        {
            checkArgument(values != null, "value is null");
            checkArgument(unchangedValues != null, "value is null");
            checkArgument(values.size() == unchangedValues.size());
            ImmutableList.Builder<RowExpression> rowExpressions = ImmutableList.builder();
            for (int i = 0; i < values.size(); i++) {
                rowExpressions.add(toRowExpression(values.get(i), unchangedValues.get(i)));
            }
            return rowExpressions.build();
        }

        private RowExpression toRowExpression(Object value, RowExpression originalRowExpression)
        {
            if (optimizationLevel.ordinal() <= SERIALIZABLE.ordinal() && !isSerializable(value, originalRowExpression.getType())) {
                return originalRowExpression;
            }
            // handle lambda
            if (optimizationLevel.ordinal() < EVALUATED.ordinal() && value instanceof MethodHandle) {
                return originalRowExpression;
            }
            return LiteralEncoder.toRowExpression(value, originalRowExpression.getType());
        }

        private boolean isSerializable(Object value, Type type)
        {
            // If value is already RowExpression, constant values contained inside should already have been made serializable. Otherwise, we make sure the object is small and serializable.
            //return value instanceof RowExpression || (isSupportedLiteralType(type) && estimatedSizeInBytes(value) <= MAX_SERIALIZABLE_OBJECT_SIZE);
            return true;
        }

        private SpecialCallResult tryHandleCast(CallExpression callExpression, List<Object> argumentValues)
        {
            checkArgument(resolution.isCastFunction(callExpression.getFunctionHandle()));
            checkArgument(callExpression.getArguments().size() == 1);
            RowExpression source = callExpression.getArguments().get(0);
            Type sourceType = source.getType();
            Type targetType = callExpression.getType();

            Object value = argumentValues.get(0);

            if (value == null) {
                return changed(null);
            }

            if (value instanceof RowExpression) {
                if (sourceType.equals(targetType)) {
                    return changed(value);
                }
                return changed(call(callExpression.getDisplayName(), callExpression.getFunctionHandle(), callExpression.getType(), toRowExpression(value, source)));
            }

            // TODO: still there is limitation for RowExpression. Example types could be Regex
            if (optimizationLevel.ordinal() <= SERIALIZABLE.ordinal() && !isSupportedLiteralType(targetType)) {
                // Otherwise, cast will be evaluated through invoke later and generates unserializable constant expression.
                return changed(call(callExpression.getDisplayName(), callExpression.getFunctionHandle(), callExpression.getType(), toRowExpression(value, source)));
            }

            if (metadata.getTypeManager().isTypeOnlyCoercion(sourceType, targetType)) {
                return changed(value);
            }
            return notChanged();
        }
    }

    static final class SpecialCallResult
    {
        private final Object value;
        private final boolean changed;

        private SpecialCallResult(Object value, boolean changed)
        {
            this.value = value;
            this.changed = changed;
        }

        public static SpecialCallResult notChanged()
        {
            return new SpecialCallResult(null, false);
        }

        public static SpecialCallResult changed(Object value)
        {
            return new SpecialCallResult(value, true);
        }

        public Object getValue()
        {
            return value;
        }

        public boolean isChanged()
        {
            return changed;
        }
    }
}