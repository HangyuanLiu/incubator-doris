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
package org.apache.doris.sql.type;


public enum OperatorType
{
    // TODO: Move out this class. Ideally this class should not be in presto-common module.

    ADD("+", false),
    SUBTRACT("-", false),
    MULTIPLY("*", false),
    DIVIDE("/", false),
    MODULUS("%", false),
    NEGATION("-", false),
    EQUAL("=", false),
    NOT_EQUAL("<>", false),
    LESS_THAN("<", false),
    LESS_THAN_OR_EQUAL("<=", false),
    GREATER_THAN(">", false),
    GREATER_THAN_OR_EQUAL(">=", false),
    BETWEEN("BETWEEN", false),
    CAST("CAST", false),
    SUBSCRIPT("[]", false),
    HASH_CODE("HASH CODE", false),
    SATURATED_FLOOR_CAST("SATURATED FLOOR CAST", false),
    IS_DISTINCT_FROM("IS DISTINCT FROM", true),
    XX_HASH_64("XX HASH 64", false),
    INDETERMINATE("INDETERMINATE", true);

    private final String operator;
    private final boolean calledOnNullInput;

    OperatorType(String operator, boolean calledOnNullInput)
    {
        this.operator = operator;
        this.calledOnNullInput = calledOnNullInput;
    }

    public String getOperator()
    {
        return operator;
    }


    public boolean isCalledOnNullInput()
    {
        return calledOnNullInput;
    }


    public boolean isComparisonOperator()
    {
        return this.equals(EQUAL) ||
                this.equals(NOT_EQUAL) ||
                this.equals(LESS_THAN) ||
                this.equals(LESS_THAN_OR_EQUAL) ||
                this.equals(GREATER_THAN) ||
                this.equals(GREATER_THAN_OR_EQUAL) ||
                this.equals(IS_DISTINCT_FROM);
    }

    public boolean isArithmeticOperator()
    {
        return this.equals(ADD) || this.equals(SUBTRACT) || this.equals(MULTIPLY) || this.equals(DIVIDE) || this.equals(MODULUS);
    }
}
