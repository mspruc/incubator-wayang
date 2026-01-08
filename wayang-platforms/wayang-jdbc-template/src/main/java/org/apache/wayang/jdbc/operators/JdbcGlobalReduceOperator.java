package org.apache.wayang.jdbc.operators;

import java.sql.Connection;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;

public abstract class JdbcGlobalReduceOperator extends GlobalReduceOperator<Record>
        implements JdbcExecutionOperator {

    public JdbcGlobalReduceOperator(GlobalReduceOperator<Record> globalReduceOperator) {
        super(globalReduceOperator);
    }

    public JdbcGlobalReduceOperator(final ReduceDescriptor<Record> reduceDescriptor) {
        super(reduceDescriptor);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.jdbc.globalreduce.load";
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return compiler.compile(reduceDescriptor);
    }
}