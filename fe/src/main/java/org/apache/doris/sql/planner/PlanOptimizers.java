package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.planner.iterative.IterativeOptimizer;
import org.apache.doris.sql.planner.iterative.rule.InlineProjections;
import org.apache.doris.sql.planner.iterative.rule.MergeLimitWithSort;
import org.apache.doris.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import org.apache.doris.sql.planner.optimizations.PlanOptimizer;
import org.apache.doris.sql.planner.iterative.rule.TranslateExpressions;
import org.apache.doris.sql.planner.optimizations.UnaliasSymbolReferences;

import java.util.List;

public class PlanOptimizers {
    private final List<PlanOptimizer> optimizers;

    public PlanOptimizers(Metadata metadata, SqlParser sqlParser) {
        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                null, null, null,
                ImmutableSet.of(
                        new InlineProjections(metadata.getFunctionManager()),
                        new RemoveRedundantIdentityProjections()));


        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        builder.add(new IterativeOptimizer(
                null, null, null,
                new TranslateExpressions(metadata, sqlParser).rules()));

        builder.add(
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
