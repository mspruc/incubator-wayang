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

package org.apache.wayang.java.mapping;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.ZipWithIdOperator;
import org.apache.wayang.core.function.ExecutionContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.operators.JavaMapOperator;
import org.apache.wayang.java.platform.JavaPlatform;

/**
 * Mapping from {@link ZipWithIdMapping} to a subplan.
 */
public class ZipWithIdMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern<?> operatorPattern = new OperatorPattern<>(
                "zipwithid", new ZipWithIdOperator<>(Serializable.class), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<ZipWithIdOperator<Serializable>>(
                (matchedOperator, epoch) -> {
                    final DataSetType<Serializable> inputType = matchedOperator.getInputType();
                    final DataSetType<Tuple2<Long, Serializable>> outputType = matchedOperator.getOutputType();
                    return new JavaMapOperator<>(
                            inputType,
                            outputType,
                            new TransformationDescriptor<>(
                                    new FunctionDescriptor.ExtendedSerializableFunction<Serializable, Tuple2<Long, Serializable>>() {

                                        private long nextId;

                                        @Override
                                        public void open(final ExecutionContext ctx) {
                                            this.nextId = 0L;
                                        }

                                        @Override
                                        public Tuple2<Long, Serializable> apply(final Serializable o) {
                                            return new Tuple2<>(this.nextId++, o);
                                        }
                                    },
                                    inputType.getDataUnitType().toBasicDataUnitType(),
                                    outputType.getDataUnitType().toBasicDataUnitType()))
                            .at(epoch);
                });
    }
}
