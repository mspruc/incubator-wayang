package org.apache.wayang.core.impl;

@FunctionalInterface
public interface ISqlImpl extends IDescriptor {
    public String getSqlClause();

    public static ISqlImpl of(final String sql) { return () -> sql; }
}
