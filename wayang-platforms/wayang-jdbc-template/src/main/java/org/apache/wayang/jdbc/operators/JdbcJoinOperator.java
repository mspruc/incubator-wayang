/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.jdbc.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.impl.ISqlImpl;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

/**
 * PostgreSQL implementation for the {@link JoinOperator}.
 */
public abstract class JdbcJoinOperator
        extends BinaryToUnaryOperator<Record, Record, Tuple2<Record, Record>>
        implements JdbcExecutionOperator {

    /*
     * 
     */
    private static DataSetType<Tuple2<Record, Record>> createOutputDataSetType() {
        return DataSetType.createDefaultUnchecked(Tuple2.class);
    }

    private final ISqlImpl keyDescriptor0;
    private final ISqlImpl keyDescriptor1;

    public ISqlImpl getKeyDescriptor0() {
        return keyDescriptor0;
    }

    public ISqlImpl getKeyDescriptor1() {
        return keyDescriptor1;
    }

    /**
     * Creates a new instance.
     *
     * @see JoinOperator#JoinOperator(Record, Record...)
     */
    protected JdbcJoinOperator(
            final ISqlImpl keyDescriptor0,
            final ISqlImpl keyDescriptor1) {
        super(DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class),
                JdbcJoinOperator.createOutputDataSetType(),
                true);

        this.keyDescriptor0 = keyDescriptor0;
        this.keyDescriptor1 = keyDescriptor1;
    }

    /**
     * Copies an instance
     *
     * @param that that should be copied
     */
    protected JdbcJoinOperator(final JdbcJoinOperator that) {
        super(DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class),
                JdbcJoinOperator.createOutputDataSetType(),
                true);
        this.keyDescriptor0 = ISqlImpl.of(that.getKeyDescriptor0().getSqlClause());
        this.keyDescriptor1 = ISqlImpl.of(that.getKeyDescriptor1().getSqlClause());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("wayang.%s.join.load", this.getPlatform().getPlatformId());
    }

}
