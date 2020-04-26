package org.apache.doris.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.QualifiedName;
import org.apache.doris.sql.tree.WithQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Scope
{
    private final Optional<Scope> parent;
    private final boolean queryBoundary;
    private final RelationId relationId;
    private final RelationType relation;
    private final Map<String, WithQuery> namedQueries;

    public static Scope create()
    {
        return builder().build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private Scope(
            Optional<Scope> parent,
            boolean queryBoundary,
            RelationId relationId,
            RelationType relation,
            Map<String, WithQuery> namedQueries)
    {
        this.parent = requireNonNull(parent, "parent is null");
        this.relationId = requireNonNull(relationId, "relationId is null");
        this.queryBoundary = queryBoundary;
        this.relation = requireNonNull(relation, "relation is null");
        this.namedQueries = ImmutableMap.copyOf(requireNonNull(namedQueries, "namedQueries is null"));
    }

    public Optional<Scope> getOuterQueryParent()
    {
        Scope scope = this;
        while (scope.parent.isPresent()) {
            if (scope.queryBoundary) {
                return scope.parent;
            }
            scope = scope.parent.get();
        }

        return Optional.empty();
    }

    public Optional<Scope> getLocalParent()
    {
        if (!queryBoundary) {
            return parent;
        }

        return Optional.empty();
    }

    public RelationId getRelationId()
    {
        return relationId;
    }

    public RelationType getRelationType()
    {
        return relation;
    }

    public ResolvedField resolveField(Expression expression, QualifiedName name)
    {
        //return tryResolveField(expression, name).orElseThrow(() -> missingAttributeException(expression, name));
        return null;
    }

    public Optional<ResolvedField> tryResolveField(Expression expression)
    {
        QualifiedName qualifiedName = asQualifiedName(expression);
        if (qualifiedName != null) {
            return tryResolveField(expression, qualifiedName);
        }
        return Optional.empty();
    }

    private static QualifiedName asQualifiedName(Expression expression)
    {
        /*
        QualifiedName name = null;
        if (expression instanceof Identifier) {
            name = QualifiedName.of(((Identifier) expression).getValue());
        }
        else if (expression instanceof DereferenceExpression) {
            name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
        }
        return name;

         */
        return null;
    }

    public Optional<ResolvedField> tryResolveField(Expression node, QualifiedName name)
    {
        return resolveField(node, name, 0, true);
    }

    private Optional<ResolvedField> resolveField(Expression node, QualifiedName name, int fieldIndexOffset, boolean local)
    {
        /*
        List<Field> matches = relation.resolveFields(name);
        if (matches.size() > 1) {
            throw ambiguousAttributeException(node, name);
        }
        else if (matches.size() == 1) {
            return Optional.of(asResolvedField(getOnlyElement(matches), fieldIndexOffset, local));
        }
        else {
            if (isColumnReference(name, relation)) {
                return Optional.empty();
            }
            if (parent.isPresent()) {
                return parent.get().resolveField(node, name, fieldIndexOffset + relation.getAllFieldCount(), local && !queryBoundary);
            }
            return Optional.empty();
        }

         */
        return Optional.empty();
    }

    private ResolvedField asResolvedField(Field field, int fieldIndexOffset, boolean local)
    {
        int relationFieldIndex = relation.indexOf(field);
        int hierarchyFieldIndex = relation.indexOf(field) + fieldIndexOffset;
        return new ResolvedField(this, field, hierarchyFieldIndex, relationFieldIndex, local);
    }

    public boolean isColumnReference(QualifiedName name)
    {
        Scope current = this;
        while (current != null) {
            if (isColumnReference(name, current.relation)) {
                return true;
            }
            current = current.parent.orElse(null);
        }

        return false;
    }

    private static boolean isColumnReference(QualifiedName name, RelationType relation)
    {
        /*
        while (name.getPrefix().isPresent()) {
            name = name.getPrefix().get();
            if (!relation.resolveFields(name).isEmpty()) {
                return true;
            }
        }

         */
        return false;
    }

    public Optional<WithQuery> getNamedQuery(String name)
    {
        if (namedQueries.containsKey(name)) {
            return Optional.of(namedQueries.get(name));
        }

        if (parent.isPresent()) {
            return parent.get().getNamedQuery(name);
        }

        return Optional.empty();
    }

    @Override
    public String toString()
    {
        return "";
    }

    public static final class Builder
    {
        private RelationId relationId = RelationId.anonymous();
        private RelationType relationType = new RelationType();
        private final Map<String, WithQuery> namedQueries = new HashMap<>();
        private Optional<Scope> parent = Optional.empty();
        private boolean queryBoundary;

        public Builder withRelationType(RelationId relationId, RelationType relationType)
        {
            this.relationId = requireNonNull(relationId, "relationId is null");
            this.relationType = requireNonNull(relationType, "relationType is null");
            return this;
        }

        public Builder withParent(Scope parent)
        {
            //checkArgument(!this.parent.isPresent(), "parent is already set");
            this.parent = Optional.of(parent);
            return this;
        }

        public Builder withOuterQueryParent(Scope parent)
        {
            //checkArgument(!this.parent.isPresent(), "parent is already set");
            this.parent = Optional.of(parent);
            this.queryBoundary = true;
            return this;
        }

        public Builder withNamedQuery(String name, WithQuery withQuery)
        {
            //checkArgument(!containsNamedQuery(name), "Query '%s' is already added", name);
            namedQueries.put(name, withQuery);
            return this;
        }

        public boolean containsNamedQuery(String name)
        {
            return namedQueries.containsKey(name);
        }

        public Scope build()
        {
            return new Scope(parent, queryBoundary, relationId, relationType, namedQueries);
        }
    }
}
