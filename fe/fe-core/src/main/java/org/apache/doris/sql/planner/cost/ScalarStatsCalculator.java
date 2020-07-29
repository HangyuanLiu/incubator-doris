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
package org.apache.doris.sql.planner.cost;

import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.ConnectorSession;
import org.apache.doris.sql.metadata.FunctionMetadata;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.ConstantExpression;
import org.apache.doris.sql.relation.InputReferenceExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.RowExpressionVisitor;
import org.apache.doris.sql.relation.SpecialFormExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.analyzer.ExpressionAnalyzer;
import org.apache.doris.sql.analyzer.Scope;
import org.apache.doris.sql.planner.ExpressionInterpreter;
import org.apache.doris.sql.relational.FunctionResolution;
import org.apache.doris.sql.relational.RowExpressionOptimizer;
import org.apache.doris.sql.tree.ArithmeticBinaryExpression;
import org.apache.doris.sql.tree.ArithmeticUnaryExpression;
import org.apache.doris.sql.tree.AstVisitor;
import org.apache.doris.sql.tree.Cast;
import org.apache.doris.sql.tree.CoalesceExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.FunctionCall;
import org.apache.doris.sql.tree.Literal;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.NodeRef;
import org.apache.doris.sql.tree.NullLiteral;
import org.apache.doris.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import org.apache.doris.sql.type.OperatorType;
import org.apache.doris.sql.type.Type;
import org.apache.doris.sql.type.TypeSignature;
import org.apache.doris.sql.type.TypeUtils;

import javax.inject.Inject;

import java.util.Map;
import java.util.OptionalDouble;

import static org.apache.doris.sql.planner.cost.StatsUtil.toStatsRepresentation;
import static org.apache.doris.sql.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.COALESCE;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.NaN;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.abs;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.relational.Expressions.isNull;
import static org.apache.doris.sql.type.OperatorType.DIVIDE;
import static org.apache.doris.sql.type.OperatorType.MODULUS;
import static org.apache.doris.sql.util.MoreMath.max;
import static org.apache.doris.sql.util.MoreMath.min;

public class ScalarStatsCalculator
{
    private final Metadata metadata;

    @Inject
    public ScalarStatsCalculator(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata can not be null");
    }

    @Deprecated
    public VariableStatsEstimate calculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics, Session session, TypeProvider types)
    {
        return new ExpressionStatsVisitor(inputStatistics, session, types).process(scalarExpression);
    }

    public VariableStatsEstimate calculate(RowExpression scalarExpression, PlanNodeStatsEstimate inputStatistics, Session session)
    {
        return scalarExpression.accept(new RowExpressionStatsVisitor(inputStatistics, session.toConnectorSession()), null);
    }

    public VariableStatsEstimate calculate(RowExpression scalarExpression, PlanNodeStatsEstimate inputStatistics, ConnectorSession session)
    {
        return scalarExpression.accept(new RowExpressionStatsVisitor(inputStatistics, session), null);
    }

    private class RowExpressionStatsVisitor
            implements RowExpressionVisitor<VariableStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final ConnectorSession session;
        private final FunctionResolution resolution = new FunctionResolution(metadata.getFunctionManager());

        public RowExpressionStatsVisitor(PlanNodeStatsEstimate input, ConnectorSession session)
        {
            this.input = requireNonNull(input, "input is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public VariableStatsEstimate visitCall(CallExpression call, Void context)
        {
            if (resolution.isNegateFunction(call.getFunctionHandle())) {
                return computeNegationStatistics(call, context);
            }

            FunctionMetadata functionMetadata = metadata.getFunctionManager().getFunctionMetadata(call.getFunctionHandle());
            if (functionMetadata.getOperatorType().map(OperatorType::isArithmeticOperator).orElse(false)) {
                return computeArithmeticBinaryStatistics(call, context);
            }

            RowExpression value = new RowExpressionOptimizer(metadata).optimize(call, OPTIMIZED, session);

            if (isNull(value)) {
                return nullStatsEstimate();
            }

            if (value instanceof ConstantExpression) {
                return value.accept(this, context);
            }

            // value is not a constant but we can still propagate estimation through cast
            if (resolution.isCastFunction(call.getFunctionHandle())) {
                return computeCastStatistics(call, context);
            }
            return VariableStatsEstimate.unknown();
        }

        @Override
        public VariableStatsEstimate visitInputReference(InputReferenceExpression reference, Void context)
        {
            throw new UnsupportedOperationException("symbol stats estimation should not reach channel mapping");
        }

        @Override
        public VariableStatsEstimate visitConstant(ConstantExpression literal, Void context)
        {
            if (literal.getValue() == null) {
                return nullStatsEstimate();
            }

            OptionalDouble doubleValue = toStatsRepresentation(metadata.getFunctionManager(), session, literal.getType(), literal.getValue());
            VariableStatsEstimate.Builder estimate = VariableStatsEstimate.builder()
                    .setNullsFraction(0)
                    .setDistinctValuesCount(1);

            if (doubleValue.isPresent()) {
                estimate.setLowValue(doubleValue.getAsDouble());
                estimate.setHighValue(doubleValue.getAsDouble());
            }
            return estimate.build();
        }

        @Override
        public VariableStatsEstimate visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return input.getVariableStatistics(reference);
        }

        @Override
        public VariableStatsEstimate visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            if (specialForm.getForm().equals(COALESCE)) {
                VariableStatsEstimate result = null;
                for (RowExpression operand : specialForm.getArguments()) {
                    VariableStatsEstimate operandEstimates = operand.accept(this, context);
                    if (result != null) {
                        result = estimateCoalesce(input, result, operandEstimates);
                    }
                    else {
                        result = operandEstimates;
                    }
                }
                return requireNonNull(result, "result is null");
            }
            return VariableStatsEstimate.unknown();
        }

        private VariableStatsEstimate computeCastStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate sourceStats = call.getArguments().get(0).accept(this, context);

            // todo - make this general postprocessing rule.
            double distinctValuesCount = sourceStats.getDistinctValuesCount();
            double lowValue = sourceStats.getLowValue();
            double highValue = sourceStats.getHighValue();

            if (TypeUtils.isIntegralType(call.getType().getTypeSignature(), metadata.getTypeManager())) {
                // todo handle low/high value changes if range gets narrower due to cast (e.g. BIGINT -> SMALLINT)
                if (isFinite(lowValue)) {
                    lowValue = Math.round(lowValue);
                }
                if (isFinite(highValue)) {
                    highValue = Math.round(highValue);
                }
                if (isFinite(lowValue) && isFinite(highValue)) {
                    double integersInRange = highValue - lowValue + 1;
                    if (!isNaN(distinctValuesCount) && distinctValuesCount > integersInRange) {
                        distinctValuesCount = integersInRange;
                    }
                }
            }

            return VariableStatsEstimate.builder()
                    .setNullsFraction(sourceStats.getNullsFraction())
                    .setLowValue(lowValue)
                    .setHighValue(highValue)
                    .setDistinctValuesCount(distinctValuesCount)
                    .build();
        }

        private VariableStatsEstimate computeNegationStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate stats = call.getArguments().get(0).accept(this, context);
            if (resolution.isNegateFunction(call.getFunctionHandle())) {
                return VariableStatsEstimate.buildFrom(stats)
                        .setLowValue(-stats.getHighValue())
                        .setHighValue(-stats.getLowValue())
                        .build();
            }
            throw new IllegalStateException(format("Unexpected sign: %s(%s)" + call.getDisplayName(), call.getFunctionHandle()));
        }

        private VariableStatsEstimate computeArithmeticBinaryStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate left = call.getArguments().get(0).accept(this, context);
            VariableStatsEstimate right = call.getArguments().get(1).accept(this, context);

            VariableStatsEstimate.Builder result = VariableStatsEstimate.builder()
                    .setAverageRowSize(Math.max(left.getAverageRowSize(), right.getAverageRowSize()))
                    .setNullsFraction(left.getNullsFraction() + right.getNullsFraction() - left.getNullsFraction() * right.getNullsFraction())
                    .setDistinctValuesCount(min(left.getDistinctValuesCount() * right.getDistinctValuesCount(), input.getOutputRowCount()));

            FunctionMetadata functionMetadata = metadata.getFunctionManager().getFunctionMetadata(call.getFunctionHandle());
            checkState(functionMetadata.getOperatorType().isPresent());
            OperatorType operatorType = functionMetadata.getOperatorType().get();
            double leftLow = left.getLowValue();
            double leftHigh = left.getHighValue();
            double rightLow = right.getLowValue();
            double rightHigh = right.getHighValue();
            if (isNaN(leftLow) || isNaN(leftHigh) || isNaN(rightLow) || isNaN(rightHigh)) {
                result.setLowValue(NaN).setHighValue(NaN);
            }
            else if (operatorType.equals(DIVIDE) && rightLow < 0 && rightHigh > 0) {
                result.setLowValue(Double.NEGATIVE_INFINITY)
                        .setHighValue(Double.POSITIVE_INFINITY);
            }
            else if (operatorType.equals(MODULUS)) {
                double maxDivisor = max(abs(rightLow), abs(rightHigh));
                if (leftHigh <= 0) {
                    result.setLowValue(max(-maxDivisor, leftLow))
                            .setHighValue(0);
                }
                else if (leftLow >= 0) {
                    result.setLowValue(0)
                            .setHighValue(min(maxDivisor, leftHigh));
                }
                else {
                    result.setLowValue(max(-maxDivisor, leftLow))
                            .setHighValue(min(maxDivisor, leftHigh));
                }
            }
            else {
                double v1 = operate(operatorType, leftLow, rightLow);
                double v2 = operate(operatorType, leftLow, rightHigh);
                double v3 = operate(operatorType, leftHigh, rightLow);
                double v4 = operate(operatorType, leftHigh, rightHigh);
                double lowValue = min(v1, v2, v3, v4);
                double highValue = max(v1, v2, v3, v4);

                result.setLowValue(lowValue)
                        .setHighValue(highValue);
            }

            return result.build();
        }

        private double operate(OperatorType operator, double left, double right)
        {
            switch (operator) {
                case ADD:
                    return left + right;
                case SUBTRACT:
                    return left - right;
                case MULTIPLY:
                    return left * right;
                case DIVIDE:
                    return left / right;
                case MODULUS:
                    return left % right;
                default:
                    throw new IllegalStateException("Unsupported ArithmeticBinaryExpression.Operator: " + operator);
            }
        }
    }

    private class ExpressionStatsVisitor
            extends AstVisitor<VariableStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final TypeProvider types;

        ExpressionStatsVisitor(PlanNodeStatsEstimate input, Session session, TypeProvider types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
        }

        @Override
        protected VariableStatsEstimate visitNode(Node node, Void context)
        {
            return VariableStatsEstimate.unknown();
        }

        @Override
        protected VariableStatsEstimate visitSymbolReference(SymbolReference node, Void context)
        {
            return input.getVariableStatistics(new VariableReferenceExpression(node.getName(), types.get(node)));
        }

        @Override
        protected VariableStatsEstimate visitNullLiteral(NullLiteral node, Void context)
        {
            return nullStatsEstimate();
        }

        @Override
        protected VariableStatsEstimate visitLiteral(Literal node, Void context)
        {
            /*
            Object value = evaluate(metadata, session.toConnectorSession(), node);
            Type type = ExpressionAnalyzer.createConstantAnalyzer(metadata, session, ImmutableList.of(), WarningCollector.NOOP).analyze(node, Scope.create());
            OptionalDouble doubleValue = toStatsRepresentation(metadata, session, type, value);
            VariableStatsEstimate.Builder estimate = VariableStatsEstimate.builder()
                    .setNullsFraction(0)
                    .setDistinctValuesCount(1);

            if (doubleValue.isPresent()) {
                estimate.setLowValue(doubleValue.getAsDouble());
                estimate.setHighValue(doubleValue.getAsDouble());
            }
            return estimate.build();
            */
            return VariableStatsEstimate.zero();
        }

        @Override
        protected VariableStatsEstimate visitFunctionCall(FunctionCall node, Void context)
        {
            /*
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, node, types);
            ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(node, metadata, session, expressionTypes);
            Object value = interpreter.optimize(NoOpVariableResolver.INSTANCE);

            if (value == null || value instanceof NullLiteral) {
                return nullStatsEstimate();
            }

            if (value instanceof Expression && !(value instanceof Literal)) {
                // value is not a constant
                return VariableStatsEstimate.unknown();
            }

            // value is a constant
            return VariableStatsEstimate.builder()
                    .setNullsFraction(0)
                    .setDistinctValuesCount(1)
                    .build();
             */
            return VariableStatsEstimate.zero();
        }

        private Map<NodeRef<Expression>, Type> getExpressionTypes(Session session, Expression expression, TypeProvider types)
        {
            ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                    metadata.getFunctionManager(),
                    metadata.getTypeManager(),
                    session,
                    types,
                    emptyList(),
                    node -> new IllegalStateException("Unexpected node: %s" + node),
                    WarningCollector.NOOP,
                    false);
            expressionAnalyzer.analyze(expression, Scope.create());
            return expressionAnalyzer.getExpressionTypes();
        }

        @Override
        protected VariableStatsEstimate visitCast(Cast node, Void context)
        {
            /*
            VariableStatsEstimate sourceStats = process(node.getExpression());
            TypeSignature targetType = TypeSignature.parseTypeSignature(node.getType());

            // todo - make this general postprocessing rule.
            double distinctValuesCount = sourceStats.getDistinctValuesCount();
            double lowValue = sourceStats.getLowValue();
            double highValue = sourceStats.getHighValue();

            if (TypeUtils.isIntegralType(targetType, metadata.getTypeManager())) {
                // todo handle low/high value changes if range gets narrower due to cast (e.g. BIGINT -> SMALLINT)
                if (isFinite(lowValue)) {
                    lowValue = Math.round(lowValue);
                }
                if (isFinite(highValue)) {
                    highValue = Math.round(highValue);
                }
                if (isFinite(lowValue) && isFinite(highValue)) {
                    double integersInRange = highValue - lowValue + 1;
                    if (!isNaN(distinctValuesCount) && distinctValuesCount > integersInRange) {
                        distinctValuesCount = integersInRange;
                    }
                }
            }

            return VariableStatsEstimate.builder()
                    .setNullsFraction(sourceStats.getNullsFraction())
                    .setLowValue(lowValue)
                    .setHighValue(highValue)
                    .setDistinctValuesCount(distinctValuesCount)
                    .build();
            */
            return VariableStatsEstimate.zero();
        }

        @Override
        protected VariableStatsEstimate visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            VariableStatsEstimate stats = process(node.getValue());
            switch (node.getSign()) {
                case PLUS:
                    return stats;
                case MINUS:
                    return VariableStatsEstimate.buildFrom(stats)
                            .setLowValue(-stats.getHighValue())
                            .setHighValue(-stats.getLowValue())
                            .build();
                default:
                    throw new IllegalStateException("Unexpected sign: " + node.getSign());
            }
        }

        @Override
        protected VariableStatsEstimate visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            requireNonNull(node, "node is null");
            VariableStatsEstimate left = process(node.getLeft());
            VariableStatsEstimate right = process(node.getRight());

            VariableStatsEstimate.Builder result = VariableStatsEstimate.builder()
                    .setAverageRowSize(Math.max(left.getAverageRowSize(), right.getAverageRowSize()))
                    .setNullsFraction(left.getNullsFraction() + right.getNullsFraction() - left.getNullsFraction() * right.getNullsFraction())
                    .setDistinctValuesCount(min(left.getDistinctValuesCount() * right.getDistinctValuesCount(), input.getOutputRowCount()));

            double leftLow = left.getLowValue();
            double leftHigh = left.getHighValue();
            double rightLow = right.getLowValue();
            double rightHigh = right.getHighValue();
            if (isNaN(leftLow) || isNaN(leftHigh) || isNaN(rightLow) || isNaN(rightHigh)) {
                result.setLowValue(NaN)
                        .setHighValue(NaN);
            }
            else if (node.getOperator() == ArithmeticBinaryExpression.Operator.DIVIDE && rightLow < 0 && rightHigh > 0) {
                result.setLowValue(Double.NEGATIVE_INFINITY)
                        .setHighValue(Double.POSITIVE_INFINITY);
            }
            else if (node.getOperator() == ArithmeticBinaryExpression.Operator.MODULUS) {
                double maxDivisor = max(abs(rightLow), abs(rightHigh));
                if (leftHigh <= 0) {
                    result.setLowValue(max(-maxDivisor, leftLow))
                            .setHighValue(0);
                }
                else if (leftLow >= 0) {
                    result.setLowValue(0)
                            .setHighValue(min(maxDivisor, leftHigh));
                }
                else {
                    result.setLowValue(max(-maxDivisor, leftLow))
                            .setHighValue(min(maxDivisor, leftHigh));
                }
            }
            else {
                double v1 = operate(node.getOperator(), leftLow, rightLow);
                double v2 = operate(node.getOperator(), leftLow, rightHigh);
                double v3 = operate(node.getOperator(), leftHigh, rightLow);
                double v4 = operate(node.getOperator(), leftHigh, rightHigh);
                double lowValue = min(v1, v2, v3, v4);
                double highValue = max(v1, v2, v3, v4);

                result.setLowValue(lowValue)
                        .setHighValue(highValue);
            }

            return result.build();
        }

        private double operate(ArithmeticBinaryExpression.Operator operator, double left, double right)
        {
            switch (operator) {
                case ADD:
                    return left + right;
                case SUBTRACT:
                    return left - right;
                case MULTIPLY:
                    return left * right;
                case DIVIDE:
                    return left / right;
                case MODULUS:
                    return left % right;
                default:
                    throw new IllegalStateException("Unsupported ArithmeticBinaryExpression.Operator: " + operator);
            }
        }

        @Override
        protected VariableStatsEstimate visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            requireNonNull(node, "node is null");
            VariableStatsEstimate result = null;
            for (Expression operand : node.getOperands()) {
                VariableStatsEstimate operandEstimates = process(operand);
                if (result != null) {
                    result = estimateCoalesce(input, result, operandEstimates);
                }
                else {
                    result = operandEstimates;
                }
            }
            return requireNonNull(result, "result is null");
        }
    }

    private static VariableStatsEstimate estimateCoalesce(PlanNodeStatsEstimate input, VariableStatsEstimate left, VariableStatsEstimate right)
    {
        // Question to reviewer: do you have a method to check if fraction is empty or saturated?
        if (left.getNullsFraction() == 0) {
            return left;
        }
        else if (left.getNullsFraction() == 1.0) {
            return right;
        }
        else {
            return VariableStatsEstimate.builder()
                    .setLowValue(min(left.getLowValue(), right.getLowValue()))
                    .setHighValue(max(left.getHighValue(), right.getHighValue()))
                    .setDistinctValuesCount(left.getDistinctValuesCount() +
                            min(right.getDistinctValuesCount(), input.getOutputRowCount() * left.getNullsFraction()))
                    .setNullsFraction(left.getNullsFraction() * right.getNullsFraction())
                    // TODO check if dataSize estimation method is correct
                    .setAverageRowSize(max(left.getAverageRowSize(), right.getAverageRowSize()))
                    .build();
        }
    }

    private static VariableStatsEstimate nullStatsEstimate()
    {
        return VariableStatsEstimate.builder()
                .setDistinctValuesCount(0)
                .setNullsFraction(1)
                .build();
    }
}
