package org.apache.doris.sql.analyzer;

import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.NodeLocation;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SemanticException
        extends RuntimeException
{
    private final SemanticErrorCode code;
    private final Node node;

    public SemanticException(SemanticErrorCode code, Node node, String format, Object... args)
    {
        super(formatMessage(format, node, args));
        requireNonNull(code, "code is null");
        requireNonNull(node, "node is null");

        this.code = code;
        this.node = node;
    }

    public Node getNode()
    {
        return node;
    }

    public SemanticErrorCode getCode()
    {
        return code;
    }

    private static String formatMessage(String formatString, Node node, Object[] args)
    {
        if (node.getLocation().isPresent()) {
            NodeLocation nodeLocation = node.getLocation().get();
            return format("line %s:%s: %s", nodeLocation.getLineNumber(), nodeLocation.getColumnNumber(), format(formatString, args));
        }
        return format(formatString, args);
    }
}
