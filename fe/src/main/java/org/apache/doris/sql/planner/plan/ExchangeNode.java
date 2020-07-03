package org.apache.doris.sql.planner.plan;

import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.List;

public class ExchangeNode
        extends LogicalPlanNode {
    public enum Type {
        GATHER,
        REPARTITION,
        REPLICATE
    }

    public enum Scope {
        LOCAL(false),
        REMOTE_STREAMING(true),
        REMOTE_MATERIALIZED(true),
        /**/;

        private boolean remote;

        Scope(boolean remote) {
            this.remote = remote;
        }

        public boolean isRemote() {
            return remote;
        }

        public boolean isLocal() {
            return !isRemote();
        }
    }

    private final Type type;
    private final Scope scope;

    private final List<LogicalPlanNode> sources;

    // for each source, the list of inputs corresponding to each output
    private final List<List<VariableReferenceExpression>> inputs;

    public ExchangeNode(PlanNodeId id, Type type, Scope scope, List<LogicalPlanNode> sources, List<List<VariableReferenceExpression>> inputs) {
        super(id);
        this.type = type;
        this.scope = scope;
        this.sources = sources;
        this.inputs = inputs;
    }

    public Type getType()
    {
        return type;
    }

    public Scope getScope()
    {
        return scope;
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        return sources;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables() {
        return inputs.get(0);
    }

    public List<List<VariableReferenceExpression>> getInputs()
    {
        return inputs;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitExchange(this, context);
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        return new ExchangeNode(getId(), type, scope, newChildren, inputs);
    }
}