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
package org.apache.doris.sql.relation;


import org.apache.doris.sql.type.Type;

import java.util.Objects;

public final class ConstantExpression
        extends RowExpression
{
    private final Object value;
    private final Type type;

    public ConstantExpression(Object value, Type type)
    {
        this.value = value;
        this.type = type;
    }

    /*
    public static ConstantExpression createConstantExpression(
            Block valueBlock,
            Type type)
    {
        return new ConstantExpression(Utils.blockToNativeValue(type, valueBlock), type);
    }

    
    public Block getValueBlock()
    {
        return Utils.nativeValueToBlock(type, value);
    }

     */

    public Object getValue()
    {
        return value;
    }

    public boolean isNull()
    {
        return value == null;
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return String.valueOf(value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ConstantExpression other = (ConstantExpression) obj;
        return Objects.equals(this.value, other.value) && Objects.equals(this.type, other.type);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitConstant(this, context);
    }
}
