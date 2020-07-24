package org.apache.doris.sql.planner;


import com.google.common.collect.ImmutableList;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.analyzer.RelationType;
import org.apache.doris.sql.analyzer.Scope;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The purpose of this class is to hold the current plan built so far
 * for a relation (query, table, values, etc.), and the mapping to
 * indicate how the fields (by position) in the relation map to
 * the outputs of the plan.
 * <p>
 * Fields are resolved by {@link TranslationMap} within local scopes hierarchy.
 * Indexes of resolved parent scope fields start from "total number of child scope fields".
 * For instance if a child scope has n fields, then first parent scope field
 * will have index n.
 */
public class RelationPlan {
    private final LogicalPlanNode root;
    private final List<VariableReferenceExpression> fieldMappings; // for each field in the relation, the corresponding variable from "root"
    private final Scope scope;

    public RelationPlan(LogicalPlanNode root, Scope scope, List<VariableReferenceExpression> fieldMappings)
    {
        requireNonNull(root, "root is null");
        requireNonNull(fieldMappings, "outputSymbols is null");
        requireNonNull(scope, "scope is null");

        int allFieldCount = getAllFieldCount(scope);
        checkArgument(allFieldCount == fieldMappings.size());
        this.root = root;
        this.scope = scope;
        this.fieldMappings = ImmutableList.copyOf(fieldMappings);
    }

    public VariableReferenceExpression getVariable(int fieldIndex)
    {
        return fieldMappings.get(fieldIndex);
    }

    public LogicalPlanNode getRoot()
    {
        return root;
    }

    public List<VariableReferenceExpression> getFieldMappings()
    {
        return fieldMappings;
    }

    public RelationType getDescriptor()
    {
        return scope.getRelationType();
    }

    public Scope getScope()
    {
        return scope;
    }

    private static int getAllFieldCount(Scope root)
    {
        int allFieldCount = 0;
        Optional<Scope> current = Optional.of(root);
        while (current.isPresent()) {
            allFieldCount += current.get().getRelationType().getAllFieldCount();
            current = current.get().getLocalParent();
        }
        return allFieldCount;
    }

}
