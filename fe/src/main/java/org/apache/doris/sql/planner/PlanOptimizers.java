package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.planner.iterative.IterativeOptimizer;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.rule.InlineProjections;
import org.apache.doris.sql.planner.iterative.rule.MergeLimitWithSort;
import org.apache.doris.sql.planner.iterative.rule.PruneTableScanColumns;
import org.apache.doris.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import org.apache.doris.sql.planner.optimizations.AddExchanges;
import org.apache.doris.sql.planner.optimizations.PlanOptimizer;
import org.apache.doris.sql.planner.iterative.rule.TranslateExpressions;
import org.apache.doris.sql.planner.optimizations.UnaliasSymbolReferences;

import java.util.List;
import java.util.Set;

public class PlanOptimizers {
    private final List<PlanOptimizer> optimizers;

    public PlanOptimizers(Metadata metadata, SqlParser sqlParser) {


        Set<Rule<?>> columnPruningRules = ImmutableSet.of(new PruneTableScanColumns());

        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                null, null, null,
                ImmutableSet.of(
                        new InlineProjections(metadata.getFunctionManager()),
                        new RemoveRedundantIdentityProjections()));


        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        builder.add(new IterativeOptimizer(
                null, null, null, columnPruningRules));

        builder.add(new IterativeOptimizer(
                null, null, null,
                new TranslateExpressions(metadata, sqlParser).rules()));

        builder.add(
                new UnaliasSymbolReferences(),
                new IterativeOptimizer(null, null, null, ImmutableSet.of(
                        new RemoveRedundantIdentityProjections())),
                inlineProjections
        );

        builder.add(new AddExchanges(metadata));
        this.optimizers = builder.build();
    }
    public List<PlanOptimizer> get()
    {
        return optimizers;
    }
}
