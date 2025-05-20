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

package org.apache.wayang.postgres.operators;

import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.jdbc.operators.JdbcGlobalReduceOperator;

/**
 * PostgreSQL implementation of the {@link GlobalReduceOperator}.
 */
public class PostgresGlobalReduceOperator<Type> extends JdbcGlobalReduceOperator<Type> implements PostgresExecutionOperator {
    public PostgresGlobalReduceOperator(final DataSetType<Type> type,
    final ReduceDescriptor<Type> reduceDescriptor) {
        super(type, reduceDescriptor);
    }

    public PostgresGlobalReduceOperator(GlobalReduceOperator<Type> operator) {
        super(operator);
    }
}