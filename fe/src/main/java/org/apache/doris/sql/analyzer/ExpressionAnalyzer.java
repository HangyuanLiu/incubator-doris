package org.apache.doris.sql.analyzer;


import org.apache.doris.analysis.Expr;
import org.apache.doris.sql.tree.Expression;

public class ExpressionAnalyzer {

    public static ExpressionAnalysis analyzeExpression(
            Scope scope,
            Analysis analysis,
            Expr expression) {
        return new ExpressionAnalysis();
    }
}
