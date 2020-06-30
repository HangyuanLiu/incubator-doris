package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.planner.iterative.IterativeOptimizer;
import org.apache.doris.sql.planner.iterative.rule.InlineProjections;
import org.apache.doris.sql.planner.iterative.rule.MergeLimitWithSort;
import org.apache.doris.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import org.apache.doris.sql.planner.optimizations.PlanOptimizer;
import org.apache.doris.sql.planner.optimizations.TranslateExpressions;
import org.apache.doris.sql.planner.optimizations.UnaliasSymbolReferences;

import java.util.List;

public class PlanOptimizers {
    private final List<PlanOptimizer> optimizers;

    public PlanOptimizers(Metadata metadata) {
        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                null, null, null,
                ImmutableSet.of(
                        new InlineProjections(metadata.getFunctionManager()),
                        new RemoveRedundantIdentityProjections()));


        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();
        builder.add(
                new TranslateExpressions(metadata, null),
                new UnaliasSymbolReferences(),
                new IterativeOptimizer(null, null, null, ImmutableSet.of(
                        new RemoveRedundantIdentityProjections(),
                        new MergeLimitWithSort())),

                new UnaliasSymbolReferences(),
                new IterativeOptimizer(null, null, null, ImmutableSet.of(
                        new RemoveRedundantIdentityProjections())),
                inlineProjections
        );
        this.optimizers = builder.build();
    }
    public List<PlanOptimizer> get()
    {
        return optimizers;
    }
}
