package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.planner.iterative.IterativeOptimizer;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.rule.InlineProjections;
import org.apache.doris.sql.planner.iterative.rule.MergeLimitWithSort;
import org.apache.doris.sql.planner.iterative.rule.MergeLimitWithTopN;
import org.apache.doris.sql.planner.iterative.rule.MergeLimits;
import org.apache.doris.sql.planner.iterative.rule.PruneAggregationColumns;
import org.apache.doris.sql.planner.iterative.rule.PruneAggregationSourceColumns;
import org.apache.doris.sql.planner.iterative.rule.PruneTableScanColumns;
import org.apache.doris.sql.planner.iterative.rule.PushLimitThroughProject;
import org.apache.doris.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import org.apache.doris.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy;
import org.apache.doris.sql.planner.iterative.rule.TransformCorrelatedLateralJoinToJoin;
import org.apache.doris.sql.planner.iterative.rule.TransformExistsApplyToLateralNode;
import org.apache.doris.sql.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import org.apache.doris.sql.planner.iterative.rule.TransformUncorrelatedLateralToJoin;
import org.apache.doris.sql.planner.optimizations.AddExchanges;
import org.apache.doris.sql.planner.optimizations.PlanOptimizer;
import org.apache.doris.sql.planner.iterative.rule.TranslateExpressions;
import org.apache.doris.sql.planner.optimizations.RowExpressionPredicatePushDown;
import org.apache.doris.sql.planner.optimizations.UnaliasSymbolReferences;

import java.util.List;
import java.util.Set;

public class PlanOptimizers {
    private final List<PlanOptimizer> optimizers;

    public PlanOptimizers(Metadata metadata, SqlParser sqlParser) {

        //列裁剪
        Set<Rule<?>> columnPruningRules = ImmutableSet.of(
                new PruneAggregationColumns(),
                new PruneAggregationSourceColumns(),
                //new PruneCrossJoinColumns(),
                //new PruneFilterColumns(),
                //new PruneJoinChildrenColumns(),
                //new PruneJoinColumns(),
                //new PruneMarkDistinctColumns(),
                //new PruneOutputColumns(),
                //new PruneProjectColumns(),
                //new PruneSemiJoinColumns(),
                //new PruneSemiJoinFilteringSourceColumns(),
                //new PruneTopNColumns(),
                //new PruneLimitColumns(),
                new PruneTableScanColumns());

        //统一裁剪冗余project
        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                null, null, null,
                ImmutableSet.of(
                        new InlineProjections(metadata.getFunctionManager()),
                        new RemoveRedundantIdentityProjections()));

        //谓词下推
        PlanOptimizer rowExpressionPredicatePushDown = new RowExpressionPredicatePushDown(metadata, sqlParser);

        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();
        builder.add(new IterativeOptimizer(null, null, null,
                columnPruningRules));

        builder.add(new IterativeOptimizer(null, null, null, ImmutableSet.of(
                new RemoveRedundantIdentityProjections(),
                new PushLimitThroughProject(),
                new MergeLimits(),
                new MergeLimitWithSort(),
                new MergeLimitWithTopN(),
                new MergeLimitWithSort(),
                new SingleDistinctAggregationToGroupBy())));


        builder.add(
                new IterativeOptimizer(null,null,null,
                        ImmutableSet.of(new TransformExistsApplyToLateralNode(metadata.getFunctionManager()))),
                new IterativeOptimizer(null,null,null,
                        ImmutableSet.of(
                                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                                new TransformCorrelatedLateralJoinToJoin())));

        builder.add(new IterativeOptimizer(
                null, null, null,
                new TranslateExpressions(metadata, sqlParser).rules()));

        builder.add(new IterativeOptimizer(null,null,null,
                ImmutableSet.of(
                        new TransformUncorrelatedLateralToJoin())));

        builder.add(rowExpressionPredicatePushDown);

        builder.add(
                new UnaliasSymbolReferences(),
                new IterativeOptimizer(null, null, null, ImmutableSet.of(
                        new RemoveRedundantIdentityProjections())),
                inlineProjections
        );


        //builder.add(new AddExchanges(metadata));
        this.optimizers = builder.build();
    }
    public List<PlanOptimizer> get()
    {
        return optimizers;
    }
}
