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
package org.apache.doris.sql.rewrite;

import com.google.common.collect.ImmutableList;
import org.apache.doris.sql.analyzer.QueryExplainer;
import org.apache.doris.sql.analyzer.SemanticException;
import org.apache.doris.sql.metadata.AccessControl;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.tree.AstVisitor;
import org.apache.doris.sql.tree.Explain;
import org.apache.doris.sql.tree.ExplainFormat;
import org.apache.doris.sql.tree.ExplainOption;
import org.apache.doris.sql.tree.ExplainType;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.Statement;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.metadata.WarningCollector.NOOP;
import static org.apache.doris.sql.tree.ExplainFormat.Type.JSON;
import static org.apache.doris.sql.tree.ExplainFormat.Type.TEXT;
import static org.apache.doris.sql.tree.ExplainType.Type.IO;
import static org.apache.doris.sql.tree.ExplainType.Type.LOGICAL;
import static org.apache.doris.sql.tree.ExplainType.Type.VALIDATE;
import static org.apache.doris.sql.util.QueryUtil.singleValueQuery;

final class ExplainRewrite
        implements StatementRewrite.Rewrite
{
    @Override
    public Statement rewrite(
            Session session,
            Metadata metadata,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement node,
            List<Expression> parameters,
            AccessControl accessControl,
            WarningCollector warningCollector)
    {
        return (Statement) new Visitor(session, parser, queryExplainer, warningCollector).process(node, null);
    }

    private static final class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final Optional<QueryExplainer> queryExplainer;

        public Visitor(
                Session session,
                SqlParser parser,
                Optional<QueryExplainer> queryExplainer,
                WarningCollector warningCollector)
        {
            this.session = requireNonNull(session, "session is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
        }

        @Override
        protected Node visitExplain(Explain node, Void context)
                throws SemanticException
        {
            if (node.isAnalyze()) {
                Statement statement = (Statement) process(node.getStatement(), context);
                return new Explain(statement, node.isAnalyze(), node.isVerbose(), node.getOptions());
            }

            ExplainType.Type planType = LOGICAL;
            ExplainFormat.Type planFormat = TEXT;
            List<ExplainOption> options = node.getOptions();

            for (ExplainOption option : options) {
                if (option instanceof ExplainType) {
                    planType = ((ExplainType) option).getType();
                    // Use JSON as the default format for EXPLAIN (TYPE IO).
                    if (planType == IO) {
                        planFormat = JSON;
                    }
                    break;
                }
            }

            for (ExplainOption option : options) {
                if (option instanceof ExplainFormat) {
                    planFormat = ((ExplainFormat) option).getType();
                    break;
                }
            }

            return getQueryPlan(node, planType, planFormat);
        }

        private Node getQueryPlan(Explain node, ExplainType.Type planType, ExplainFormat.Type planFormat)
                throws IllegalArgumentException
        {
            /*
            if (planType == VALIDATE) {
                queryExplainer.get().analyze(session, node.getStatement(), warningCollector);
                return singleValueQuery("Valid", true);
            }
            */
            String plan;
            switch (planFormat) {
                /*
                case GRAPHVIZ:
                    plan = queryExplainer.get().getGraphvizPlan(session, preparedQuery.getStatement(), planType, preparedQuery.getParameters(), warningCollector);
                    break;
                case JSON:
                    plan = queryExplainer.get().getJsonPlan(session, preparedQuery.getStatement(), planType, preparedQuery.getParameters(), warningCollector);
                    break;
                    */
                case TEXT:
                    try {
                        plan = queryExplainer.get().getPlan(session, node.getStatement(), planType, ImmutableList.of(), node.isVerbose(), NOOP);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        return null;
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Invalid Explain Format: " + planFormat.toString());
            }
            return singleValueQuery("Query Plan", plan);
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
