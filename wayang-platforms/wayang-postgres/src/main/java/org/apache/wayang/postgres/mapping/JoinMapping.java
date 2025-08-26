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

package org.apache.wayang.postgres.mapping;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.impl.ISqlImpl;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.postgres.operators.PostgresJoinOperator;
import org.apache.wayang.postgres.platform.PostgresPlatform;

/**
 * Mapping from {@link JoinOperator} to {@link PostgresJoinOperator}.
 */
public class JoinMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                PostgresPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern<JoinOperator<Record, Record, Serializable>> operatorPattern = new OperatorPattern<>(
                "join",
                new JoinOperator<>(
                        null,
                        null,
                        DataSetType.createDefault(Record.class),
                        DataSetType.createDefault(Record.class)),
                false)
                .withAdditionalTest(op -> op.getKeyDescriptor0() instanceof ISqlImpl)
                .withAdditionalTest(op -> op.getKeyDescriptor1() instanceof ISqlImpl);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<JoinOperator<Record, Record, Serializable>>(
                (matchedOperator, epoch) -> new PostgresJoinOperator((ISqlImpl) matchedOperator.getKeyDescriptor0(), (ISqlImpl) matchedOperator.getKeyDescriptor1())
                        .at(epoch));
    }
}
