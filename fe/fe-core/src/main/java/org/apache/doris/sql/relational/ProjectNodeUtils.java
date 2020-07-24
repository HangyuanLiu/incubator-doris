package org.apache.doris.sql.relational;

import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.SymbolReference;

import java.util.Map;

public class ProjectNodeUtils
{
    private ProjectNodeUtils() {}

    public static boolean isIdentity(ProjectNode projectNode)
    {
        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : projectNode.getAssignments().entrySet()) {
            RowExpression value = entry.getValue();
            VariableReferenceExpression variable = entry.getKey();
            if (!(value instanceof VariableReferenceExpression && ((VariableReferenceExpression) value).getName().equals(variable.getName()))) {
                return false;
            }
        }
        return true;
    }
}