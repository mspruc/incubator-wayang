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

package org.apache.wayang.basic.operators;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.lang3.Validate;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.impl.IJavaImpl;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

/**
 * This operator returns the cartesian product of elements of input datasets.
 */
public class JoinOperator<I0 extends Serializable, I1 extends Serializable, K extends Serializable>
        extends BinaryToUnaryOperator<I0, I1, Tuple2<I0, I1>> {


    private static <I0 extends Serializable, I1 extends Serializable> DataSetType<Tuple2<I0, I1>> createOutputDataSetType() {
        return DataSetType.createDefaultUnchecked(Tuple2.class);
    }

    protected final IJavaImpl<SerializableFunction<I0, K>> keyDescriptor0;
    protected final IJavaImpl<SerializableFunction<I1, K>> keyDescriptor1;

    public JoinOperator(final SerializableFunction<I0, K> keyExtractor0,
            final SerializableFunction<I1, K> keyExtractor1,
            final Class<I0> input0Class,
            final Class<I1> input1Class) {
        super(DataSetType.createDefault(input0Class),
                DataSetType.createDefault(input1Class),
                JoinOperator.createOutputDataSetType(),
                true);
        this.keyDescriptor0 = IJavaImpl.of(keyExtractor0);
        this.keyDescriptor1 = IJavaImpl.of(keyExtractor1);
    }

    public JoinOperator(final SerializableFunction<I0, K> keyExtractor0,
            final SerializableFunction<I1, K> keyExtractor1,
            final Class<I0> input0Class,
            final Class<I1> input1Class,
            final Class<K> keyClass) {
        this(new TransformationDescriptor<>(keyExtractor0, input0Class, keyClass),
                new TransformationDescriptor<>(keyExtractor1, input1Class, keyClass));
    }

    public JoinOperator(final DataSetType<I0> input0Class,
            final DataSetType<I1> input1Class,
            final IJavaImpl<SerializableFunction<I0, K>> keyExtractor0,
            final IJavaImpl<SerializableFunction<I1, K>> keyExtractor1) {
        super(input0Class,
                input1Class,
                JoinOperator.createOutputDataSetType(),
                true);
        this.keyDescriptor0 = keyExtractor0;
        this.keyDescriptor1 = keyExtractor1;
    }

    public JoinOperator(final IJavaImpl<SerializableFunction<I0, K>> keyExtractor0,
            final IJavaImpl<SerializableFunction<I1, K>> keyExtractor1,
            final Class<I0> input0Class,
            final Class<I1> input1Class) {
        super(DataSetType.createDefault(input0Class),
                DataSetType.createDefault(input1Class),
                JoinOperator.createOutputDataSetType(),
                true);
        this.keyDescriptor0 = keyExtractor0;
        this.keyDescriptor1 = keyExtractor1;
    }

    public JoinOperator(final TransformationDescriptor<I0, K> keyDescriptor0,
            final TransformationDescriptor<I1, K> keyDescriptor1) {
        super(DataSetType.createDefault(keyDescriptor0.getInputType()),
                DataSetType.createDefault(keyDescriptor1.getInputType()),
                JoinOperator.createOutputDataSetType(),
                true);
        this.keyDescriptor0 = IJavaImpl.of(keyDescriptor0.getJavaImplementation()::apply);
        this.keyDescriptor1 = IJavaImpl.of(keyDescriptor1.getJavaImplementation()::apply);
    }

    public JoinOperator(final TransformationDescriptor<I0, K> keyDescriptor0,
            final TransformationDescriptor<I1, K> keyDescriptor1,
            final DataSetType<I0> inputType0,
            final DataSetType<I1> inputType1) {
        super(inputType0, inputType1, JoinOperator.createOutputDataSetType(), true);
        this.keyDescriptor0 = IJavaImpl.of(keyDescriptor0.getJavaImplementation()::apply);
        this.keyDescriptor1 = IJavaImpl.of(keyDescriptor1.getJavaImplementation()::apply);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JoinOperator(final JoinOperator<I0, I1, K> that) {
        super(that);
        this.keyDescriptor0 = that.getKeyDescriptor0();
        this.keyDescriptor1 = that.getKeyDescriptor1();
    }

    public IJavaImpl<SerializableFunction<I0, K>> getKeyDescriptor0() {
        return this.keyDescriptor0;
    }

    public IJavaImpl<SerializableFunction<I1, K>> getKeyDescriptor1() {
        return this.keyDescriptor1;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1L, outputIndex);
        // The current idea: We assume, we have a foreign-key like join
        // TODO: Find a better estimator.
        return Optional.of(new DefaultCardinalityEstimator(
                .5d, 2, this.isSupportingBroadcastInputs(),
                inputCards -> 3 * Math.max(inputCards[0], inputCards[1])));
    }
}
