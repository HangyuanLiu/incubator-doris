package org.apache.doris.sql.metadata;

import java.nio.channels.SeekableByteChannel;
import java.util.Map;

public class FullConnectorSession
        implements ConnectorSession {
    private final Session session;
    private final ConnectorId connectorId;

    public FullConnectorSession(Session session, ConnectorId connectorId) {
        this.session = session;
        this.connectorId = connectorId;
    }

    public Session getSession() {
        return session;
    }

    public ConnectorId getConnectorId() {
        return connectorId;
    }
}
