package org.apache.doris.sql.metadata;

public interface WarningCollector
{
    WarningCollector NOOP =
            new WarningCollector()
            {
            };
}
