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
package org.apache.doris.sql.planner.optimizations;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.FunctionHandle;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.QualifiedFunctionName;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.ConstantExpression;
import org.apache.doris.sql.relation.InputReferenceExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.RowExpressionVisitor;
import org.apache.doris.sql.relation.SpecialFormExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.NodeRef;
import org.apache.doris.sql.type.Type;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


import static com.google.common.base.Preconditions.checkArgument;

import static java.lang.Integer.min;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static org.apache.doris.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.AND;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.OR;
import static org.apache.doris.sql.relational.SqlToRowExpressionTranslator.translate;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.EQUAL;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static org.apache.doris.sql.type.BooleanType.BOOLEAN;
import static org.apache.doris.sql.type.OperatorType.LESS_THAN;
import static org.apache.doris.sql.type.OperatorType.LESS_THAN_OR_EQUAL;

public class ExpressionEquivalence {
    private static final Ordering<RowExpression> ROW_EXPRESSION_ORDERING = Ordering.from(new RowExpressionComparator());
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final CanonicalizationVisitor canonicalizationVisitor;

    public ExpressionEquivalence(Metadata metadata, SqlParser sqlParser) {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.canonicalizationVisitor = new CanonicalizationVisitor(metadata.getFunctionManager());
    }

    public boolean areExpressionsEquivalent(Session session, Expression leftExpression, Expression rightExpression, TypeProvider types) {
        Map<VariableReferenceExpression, Integer> variableInput = new HashMap<>();
        int inputId = 0;
        for (VariableReferenceExpression variable : types.allVariables()) {
            variableInput.put(variable, inputId);
            inputId++;
        }
        RowExpression leftRowExpression = toRowExpression(session, leftExpression, variableInput, types);
        RowExpression rightRowExpression = toRowExpression(session, rightExpression, variableInput, types);

        RowExpression canonicalizedLeft = leftRowExpression.accept(canonicalizationVisitor, null);
        RowExpression canonicalizedRight = rightRowExpression.accept(canonicalizationVisitor, null);

        return canonicalizedLeft.equals(canonicalizedRight);
    }

    public boolean areExpressionsEquivalent(RowExpression leftExpression, RowExpression rightExpression) {
        RowExpression canonicalizedLeft = leftExpression.accept(canonicalizationVisitor, null);
        RowExpression canonicalizedRight = rightExpression.accept(canonicalizationVisitor, null);

        return canonicalizedLeft.equals(canonicalizedRight);
    }

    private RowExpression toRowExpression(Session session, Expression expression, Map<VariableReferenceExpression, Integer> variableInput, TypeProvider types) {
        // replace qualified names with input references since row expressions do not support these

        // determine the type of every expression
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                session,
                metadata,
                sqlParser,
                types,
                expression,
                emptyList(), /* parameters have already been replaced */
                WarningCollector.NOOP);

        // convert to row expression
        return translate(expression, expressionTypes, variableInput, metadata.getFunctionManager(), metadata.getTypeManager());
    }

    private static class CanonicalizationVisitor
            implements RowExpressionVisitor<RowExpression, Void> {
        private final FunctionManager functionManager;

        public CanonicalizationVisitor(FunctionManager functionManager) {
            this.functionManager = requireNonNull(functionManager, "functionManager is null");
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context) {
            call = new CallExpression(
                    call.getDisplayName(),
                    call.getFunctionHandle(),
                    call.getType(),
                    call.getArguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(Collectors.toList()));

            QualifiedFunctionName callName = functionManager.getFunctionMetadata(call.getFunctionHandle()).getName();
            /*
            if (callName.equals(EQUAL.getFunctionName()) || callName.equals(NOT_EQUAL.getFunctionName()) || callName.equals(IS_DISTINCT_FROM.getFunctionName())) {
                // sort arguments
                return new CallExpression(
                        call.getDisplayName(),
                        call.getFunctionHandle(),
                        call.getType(),
                        ROW_EXPRESSION_ORDERING.sortedCopy(call.getArguments()));
            }

            if (callName.equals(GREATER_THAN.getFunctionName()) || callName.equals(GREATER_THAN_OR_EQUAL.getFunctionName())) {
                // convert greater than to less than

                FunctionHandle functionHandle = functionManager.resolveOperator(
                        callName.equals(GREATER_THAN.getFunctionName()) ? LESS_THAN : LESS_THAN_OR_EQUAL,
                        swapPair(fromTypes(call.getArguments().stream().map(RowExpression::getType).collect(Collectors.toList()))));
                return new CallExpression(
                        call.getDisplayName(),
                        functionHandle,
                        call.getType(),
                        swapPair(call.getArguments()));
            }
            */
            return call;
        }

        public static List<RowExpression> flattenNestedSpecialForms(SpecialFormExpression specialForm)
        {
            SpecialFormExpression.Form form = specialForm.getForm();
            ImmutableList.Builder<RowExpression> newArguments = ImmutableList.builder();
            for (RowExpression argument : specialForm.getArguments()) {
                if (argument instanceof SpecialFormExpression && form.equals(((SpecialFormExpression) argument).getForm())) {
                    // same special form type, so flatten the args
                    newArguments.addAll(flattenNestedSpecialForms((SpecialFormExpression) argument));
                } else {
                    newArguments.add(argument);
                }
            }
            return newArguments.build();
        }

        @Override
        public RowExpression visitConstant(ConstantExpression constant, Void context) {
            return constant;
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression node, Void context) {
            return node;
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context) {
            return reference;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Void context) {
            specialForm = new SpecialFormExpression(
                    specialForm.getForm(),
                    specialForm.getType(),
                    specialForm.getArguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(Collectors.toList()));

            if (specialForm.getForm() == AND || specialForm.getForm() == OR) {
                // if we have nested calls (of the same type) flatten them
                List<RowExpression> flattenedArguments = flattenNestedSpecialForms(specialForm);

                // only consider distinct arguments
                Set<RowExpression> distinctArguments = ImmutableSet.copyOf(flattenedArguments);
                if (distinctArguments.size() == 1) {
                    return Iterables.getOnlyElement(distinctArguments);
                }

                // canonicalize the argument order (i.e., sort them)
                List<RowExpression> sortedArguments = ROW_EXPRESSION_ORDERING.sortedCopy(distinctArguments);

                return new SpecialFormExpression(specialForm.getForm(), BOOLEAN, sortedArguments);
            }
            return specialForm;
        }
    }

    private static class RowExpressionComparator
            implements Comparator<RowExpression> {
        private final Comparator<Object> classComparator = Ordering.arbitrary();
        private final ListComparator<RowExpression> argumentComparator = new ListComparator<>(this);

        @Override
        public int compare(RowExpression left, RowExpression right) {
            int result = classComparator.compare(left.getClass(), right.getClass());
            if (result != 0) {
                return result;
            }

            if (left instanceof CallExpression) {
                CallExpression leftCall = (CallExpression) left;
                CallExpression rightCall = (CallExpression) right;
                return ComparisonChain.start()
                        .compare(leftCall.getFunctionHandle().toString(), rightCall.getFunctionHandle().toString())
                        .compare(leftCall.getArguments(), rightCall.getArguments(), argumentComparator)
                        .result();
            }

            if (left instanceof ConstantExpression) {
                ConstantExpression leftConstant = (ConstantExpression) left;
                ConstantExpression rightConstant = (ConstantExpression) right;

                result = leftConstant.getType().getTypeSignature().toString().compareTo(right.getType().getTypeSignature().toString());
                if (result != 0) {
                    return result;
                }

                Object leftValue = leftConstant.getValue();
                Object rightValue = rightConstant.getValue();

                if (leftValue == null) {
                    if (rightValue == null) {
                        return 0;
                    } else {
                        return -1;
                    }
                } else if (rightValue == null) {
                    return 1;
                }
                /*
                Class<?> javaType = leftConstant.getType().getJavaType();
                if (javaType == boolean.class) {
                    return ((Boolean) leftValue).compareTo((Boolean) rightValue);
                }
                if (javaType == byte.class || javaType == short.class || javaType == int.class || javaType == long.class) {
                    return Long.compare(((Number) leftValue).longValue(), ((Number) rightValue).longValue());
                }
                if (javaType == float.class || javaType == double.class) {
                    return Double.compare(((Number) leftValue).doubleValue(), ((Number) rightValue).doubleValue());
                }
                if (javaType == Slice.class) {
                    return ((Slice) leftValue).compareTo((Slice) rightValue);
                }
                */
                // value is some random type (say regex), so we just randomly choose a greater value
                // todo: support all known type
                return -1;
            }

            if (left instanceof InputReferenceExpression) {
                return Integer.compare(((InputReferenceExpression) left).getField(), ((InputReferenceExpression) right).getField());
            }

            if (left instanceof VariableReferenceExpression) {
                VariableReferenceExpression leftVariableReference = (VariableReferenceExpression) left;
                VariableReferenceExpression rightVariableReference = (VariableReferenceExpression) right;

                return ComparisonChain.start()
                        .compare(leftVariableReference.getName(), rightVariableReference.getName())
                        .compare(leftVariableReference.getType(), rightVariableReference.getType(), Comparator.comparing(Object::toString))
                        .result();
            }

            if (left instanceof SpecialFormExpression) {
                SpecialFormExpression leftSpecialForm = (SpecialFormExpression) left;
                SpecialFormExpression rightSpecialForm = (SpecialFormExpression) right;

                return ComparisonChain.start()
                        .compare(leftSpecialForm.getForm(), rightSpecialForm.getForm())
                        .compare(leftSpecialForm.getType(), rightSpecialForm.getType(), Comparator.comparing(Object::toString))
                        .compare(leftSpecialForm.getArguments(), rightSpecialForm.getArguments(), argumentComparator)
                        .result();
            }

            throw new IllegalArgumentException("Unsupported RowExpression type " + left.getClass().getSimpleName());
        }
    }

    private static class ListComparator<T>
            implements Comparator<List<T>> {
        private final Comparator<T> elementComparator;

        public ListComparator(Comparator<T> elementComparator) {
            this.elementComparator = requireNonNull(elementComparator, "elementComparator is null");
        }

        @Override
        public int compare(List<T> left, List<T> right) {
            int compareLength = min(left.size(), right.size());
            for (int i = 0; i < compareLength; i++) {
                int result = elementComparator.compare(left.get(i), right.get(i));
                if (result != 0) {
                    return result;
                }
            }
            return Integer.compare(left.size(), right.size());
        }
    }

    private static <T> List<T> swapPair(List<T> pair) {
        requireNonNull(pair, "pair is null");
        checkArgument(pair.size() == 2, "Expected pair to have two elements");
        return ImmutableList.of(pair.get(1), pair.get(0));
    }
}