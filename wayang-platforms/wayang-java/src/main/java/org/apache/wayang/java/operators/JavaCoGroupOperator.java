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

package org.apache.wayang.java.operators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.wayang.basic.operators.CoGroupOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

/**
 * Java implementation of the {@link CoGroupOperator}.
 */
public class JavaCoGroupOperator<I0 extends Serializable, I1 extends Serializable, K extends Serializable>
        extends CoGroupOperator<I0, I1, K>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     */
    public JavaCoGroupOperator(final DataSetType<I0> inputType0,
            final DataSetType<I1> inputType1,
            final TransformationDescriptor<I0, K> keyDescriptor0,
            final TransformationDescriptor<I1, K> keyDescriptor1) {

        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaCoGroupOperator(final CoGroupOperator<I0, I1, K> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final JavaExecutor javaExecutor,
            final OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<I0, K> keyExtractor0 = javaExecutor.getCompiler().compile(this.keyDescriptor0);
        final Function<I1, K> keyExtractor1 = javaExecutor.getCompiler().compile(this.keyDescriptor1);

        // Group input 0.
        final CardinalityEstimate cardinalityEstimate0 = operatorContext.getInputCardinality(0);
        final int expectedNumElements0 = (int) cardinalityEstimate0.getGeometricMeanEstimate();
        final Map<K, Collection<I0>> groups0 = new HashMap<>(expectedNumElements0);
        ((JavaChannelInstance) inputs[0]).<I0>provideStream()
                .forEach(dataQuantum0 -> groups0.compute(keyExtractor0.apply(dataQuantum0),
                        (key, value) -> {
                            value = value == null ? new LinkedList<>() : value;
                            value.add(dataQuantum0);
                            return value;
                        }));

        // Group input 1.
        final CardinalityEstimate cardinalityEstimate1 = operatorContext.getInputCardinality(1);
        final int expectedNumElements1 = (int) cardinalityEstimate1.getGeometricMeanEstimate();
        final Map<K, Collection<I1>> groups1 = new HashMap<>(expectedNumElements1);
        ((JavaChannelInstance) inputs[1]).<I1>provideStream()
                .forEach(dataQuantum1 -> groups1.compute(keyExtractor1.apply(dataQuantum1),
                        (key, value) -> {
                            value = value == null ? new LinkedList<>() : value;
                            value.add(dataQuantum1);
                            return value;
                        }));

        // Create the co-groups.
        final Collection<Tuple<Collection<I0>, Collection<I1>>> coGroups = new ArrayList<>(
                expectedNumElements0 + expectedNumElements1);
        for (final Map.Entry<K, Collection<I0>> entry : groups0.entrySet()) {
            final Collection<I0> group0 = entry.getValue();
            final Collection<I1> group1 = groups1.remove(entry.getKey());
            coGroups.add(new Tuple<>(
                    group0,
                    group1 == null ? Collections.emptyList() : group1));
        }
        for (final Collection<I1> group1 : groups1.values()) {
            coGroups.add(new Tuple<>(Collections.emptyList(), group1));
        }
        ((CollectionChannel.Instance) outputs[0]).accept(coGroups);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Collections.singletonList("wayang.java.cogroup.load");
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(final Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator = JavaExecutionOperator.super.createLoadProfileEstimator(
                configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor0, configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor1, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaCoGroupOperator<>(this.getInputType0(), this.getInputType1(), this.getKeyDescriptor0(), this.getKeyDescriptor1());
    }
}
