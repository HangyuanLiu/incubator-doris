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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.doris.sql.analyzer.SemanticErrorCode;
import org.apache.doris.sql.analyzer.SemanticException;
import org.apache.doris.sql.analyzer.TypeSignatureProvider;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.metadata.QualifiedFunctionName;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.SpecialFormExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.tree.ArithmeticBinaryExpression;
import org.apache.doris.sql.tree.ArithmeticUnaryExpression;
import org.apache.doris.sql.tree.AstVisitor;
import org.apache.doris.sql.tree.BooleanLiteral;
import org.apache.doris.sql.tree.Cast;
import org.apache.doris.sql.tree.ComparisonExpression;
import org.apache.doris.sql.tree.DecimalLiteral;
import org.apache.doris.sql.tree.DoubleLiteral;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.FieldReference;
import org.apache.doris.sql.tree.FunctionCall;
import org.apache.doris.sql.tree.GenericLiteral;
import org.apache.doris.sql.tree.Identifier;
import org.apache.doris.sql.tree.InListExpression;
import org.apache.doris.sql.tree.InPredicate;
import org.apache.doris.sql.tree.LikePredicate;
import org.apache.doris.sql.tree.LogicalBinaryExpression;
import org.apache.doris.sql.tree.LongLiteral;
import org.apache.doris.sql.tree.NodeRef;
import org.apache.doris.sql.tree.NullLiteral;
import org.apache.doris.sql.tree.QualifiedName;
import org.apache.doris.sql.tree.StringLiteral;
import org.apache.doris.sql.tree.SymbolReference;
import org.apache.doris.sql.type.BigintType;
import org.apache.doris.sql.type.BooleanType;
import org.apache.doris.sql.type.CharType;
import org.apache.doris.sql.type.DecimalParseResult;
import org.apache.doris.sql.type.Decimals;
import org.apache.doris.sql.type.OperatorType;
import org.apache.doris.sql.type.Type;
import org.apache.doris.sql.type.TypeManager;
import org.apache.doris.sql.type.TypeSignature;
import org.apache.doris.sql.type.UnknownType;
import org.apache.doris.sql.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.AND;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.IN;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.OR;
import static org.apache.doris.sql.relational.Expressions.call;
import static org.apache.doris.sql.relational.Expressions.constant;
import static org.apache.doris.sql.relational.Expressions.constantNull;
import static org.apache.doris.sql.relational.Expressions.field;
import static org.apache.doris.sql.relational.Expressions.specialForm;
import static org.apache.doris.sql.type.BigintType.BIGINT;
import static org.apache.doris.sql.type.BooleanType.BOOLEAN;
import static org.apache.doris.sql.type.DoubleType.DOUBLE;
import static org.apache.doris.sql.type.IntegerType.INTEGER;
import static org.apache.doris.sql.type.OperatorType.EQUAL;
import static org.apache.doris.sql.type.OperatorType.NEGATION;
import static org.apache.doris.sql.type.SmallintType.SMALLINT;
import static org.apache.doris.sql.type.TinyintType.TINYINT;
import static org.apache.doris.sql.type.VarcharType.VARCHAR;
import static org.apache.doris.sql.type.VarcharType.createVarcharType;

public final class SqlToRowExpressionTranslator
{
    private SqlToRowExpressionTranslator() {}

    public static RowExpression translate(
            Expression expression,
            Map<NodeRef<Expression>, Type> types,
            Map<VariableReferenceExpression, Integer> layout,
            FunctionManager functionManager,
            TypeManager typeManager)
    {
        Visitor visitor = new Visitor(
                types,
                layout,
                typeManager,
                functionManager);
        RowExpression result = visitor.process(expression, null);
        requireNonNull(result, "translated expression is null");
        return result;
    }

    private static class Visitor
            extends AstVisitor<RowExpression, Void>
    {
        private final Map<NodeRef<Expression>, Type> types;
        private final Map<VariableReferenceExpression, Integer> layout;
        private final TypeManager typeManager;
        private final FunctionManager functionManager;
        private final FunctionResolution functionResolution;

        private Visitor(
                Map<NodeRef<Expression>, Type> types,
                Map<VariableReferenceExpression, Integer> layout,
                TypeManager typeManager,
                FunctionManager functionManager)
        {
            this.types = ImmutableMap.copyOf(requireNonNull(types, "types is null"));
            this.layout = layout;
            this.typeManager = typeManager;
            this.functionManager = functionManager;
            this.functionResolution = new FunctionResolution(functionManager);
        }

        private Type getType(Expression node)
        {
            return types.get(NodeRef.of(node));
        }

        @Override
        protected RowExpression visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
        }

        @Override
        protected RowExpression visitIdentifier(Identifier node, Void context)
        {
            // identifier should never be reachable with the exception of lambda within VALUES (#9711)
            return new VariableReferenceExpression(node.getValue(), getType(node));
        }


        @Override
        protected RowExpression visitFieldReference(FieldReference node, Void context)
        {
            return field(node.getFieldIndex(), getType(node));
        }

        @Override
        protected RowExpression visitNullLiteral(NullLiteral node, Void context)
        {
            return constantNull(UnknownType.UNKNOWN);
        }

        @Override
        protected RowExpression visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return constant(node.getValue(), BOOLEAN);
        }

        @Override
        protected RowExpression visitLongLiteral(LongLiteral node, Void context)
        {
            /*
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                return constant(node.getValue(), INTEGER);
            }*/
            return constant(node.getValue(), BIGINT);
        }

        @Override
        protected RowExpression visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return constant(node.getValue(), DOUBLE);
        }

        @Override
        protected RowExpression visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            DecimalParseResult parseResult = Decimals.parse(node.getValue());
            return constant(parseResult.getObject(), parseResult.getType());
        }

        @Override
        protected RowExpression visitStringLiteral(StringLiteral node, Void context)
        {
            return constant(node.getValue(), createVarcharType(node.getValue().length()));
        }

        @Override
        protected RowExpression visitGenericLiteral(GenericLiteral node, Void context)
        {
            Type type;
            try {
                type = typeManager.getType(new TypeSignature(node.getType()));
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unsupported type: " + node.getType());
            }

            try {
                if (TINYINT.equals(type)) {
                    return constant((long) Byte.parseByte(node.getValue()), TINYINT);
                }
                else if (SMALLINT.equals(type)) {
                    return constant((long) Short.parseShort(node.getValue()), SMALLINT);
                }
                else if (BIGINT.equals(type)) {
                    return constant(Long.parseLong(node.getValue()), BIGINT);
                }
            }
            catch (NumberFormatException e) {
                throw new SemanticException(SemanticErrorCode.INVALID_LITERAL, node, format("Invalid formatted generic %s literal: %s", type, node));
            }

            return call(
                    "CAST",
                    functionManager.lookupCast(VARCHAR.getTypeSignature(), getType(node).getTypeSignature()),
                    getType(node),
                    constant(node.getValue(), VARCHAR));
        }

        @Override
        protected RowExpression visitComparisonExpression(ComparisonExpression node, Void context)
        {
            RowExpression left = process(node.getLeft(), context);
            RowExpression right = process(node.getRight(), context);

            return call(
                    node.getOperator().name(),
                    functionResolution.comparisonFunction(node.getOperator(), left.getType(), right.getType()),
                    BOOLEAN,
                    left,
                    right);
        }

        @Override
        protected RowExpression visitFunctionCall(FunctionCall node, Void context)
        {
            List<RowExpression> arguments = node.getArguments().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());

            List<TypeSignatureProvider> argumentTypes = arguments.stream()
                    .map(RowExpression::getType)
                    .map(Type::getTypeSignature)
                    .map(TypeSignatureProvider::new)
                    .collect(toImmutableList());

            return call(node.getName().toString(), functionManager.resolveFunction(node.getName(), argumentTypes), getType(node), arguments);
        }

        @Override
        protected RowExpression visitSymbolReference(SymbolReference node, Void context)
        {
            VariableReferenceExpression variable = new VariableReferenceExpression(node.getName(), getType(node));
            Integer channel = layout.get(variable);
            if (channel != null) {
                return field(channel, variable.getType());
            }

            return variable;
        }

        @Override
        protected RowExpression visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {

            RowExpression left = process(node.getLeft(), context);
            RowExpression right = process(node.getRight(), context);

            return call(
                    node.getOperator().name(),
                    functionResolution.arithmeticFunction(node.getOperator(), left.getType(), right.getType()),
                    getType(node),
                    left,
                    right);
        }

        @Override
        protected RowExpression visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            RowExpression expression = process(node.getValue(), context);

            switch (node.getSign()) {
                case PLUS:
                    return expression;
                case MINUS:
                    return call(
                            NEGATION.name(),
                            functionManager.resolveOperator(NEGATION, fromTypes(expression.getType())),
                            getType(node),
                            expression);
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
        }

        @Override
        protected RowExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            SpecialFormExpression.Form form;
            switch (node.getOperator()) {
                case AND:
                    form = AND;
                    break;
                case OR:
                    form = OR;
                    break;
                default:
                    throw new IllegalStateException("Unknown logical operator: " + node.getOperator());
            }
            return specialForm(form, BOOLEAN, process(node.getLeft(), context), process(node.getRight(), context));
        }

        private RowExpression buildEquals(RowExpression lhs, RowExpression rhs)
        {
            return call(
                    EQUAL.getOperator(),
                    functionResolution.comparisonFunction(ComparisonExpression.Operator.EQUAL, lhs.getType(), rhs.getType()),
                    BOOLEAN,
                    lhs,
                    rhs);
        }

        @Override
        protected RowExpression visitInPredicate(InPredicate node, Void context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
            RowExpression value = process(node.getValue(), context);
            InListExpression values = (InListExpression) node.getValueList();

            if (values.getValues().size() == 1) {
                return buildEquals(value, process(values.getValues().get(0), context));
            }

            arguments.add(value);
            for (Expression inValue : values.getValues()) {
                arguments.add(process(inValue, context));
            }

            return specialForm(IN, BOOLEAN, arguments.build());
        }

        @Override
        protected RowExpression visitLikePredicate(LikePredicate node, Void context)
        {
            RowExpression value = process(node.getValue(), context);
            RowExpression pattern = process(node.getPattern(), context);

            return call("LIKE", functionManager.resolveFunction(QualifiedName.of("LIKE"),
                    fromTypes(value.getType(), pattern.getType())), getType(node), Lists.newArrayList(value, pattern));

        }
    }
}
