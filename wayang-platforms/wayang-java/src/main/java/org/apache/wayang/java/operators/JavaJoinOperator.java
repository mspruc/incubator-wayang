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

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.impl.IJavaImpl;
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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link JoinOperator}.
 */
public class JavaJoinOperator<I0 extends Serializable, I1 extends Serializable, K extends Serializable>
        extends JoinOperator<I0, I1, K>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     */
    public JavaJoinOperator(final DataSetType<I0> inputType0,
            final DataSetType<I1> inputType1,
            final TransformationDescriptor<I0, K> keyDescriptor0,
            final TransformationDescriptor<I1, K> keyDescriptor1) {

        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    /**
     * Creates a new instance.
     * 
     * @param inputType0
     * @param inputType1
     * @param keyDescriptor0
     * @param keyDescriptor1
     */
    public JavaJoinOperator(
            final IJavaImpl<SerializableFunction<I0, K>> keyDescriptor0,
            final IJavaImpl<SerializableFunction<I1, K>> keyDescriptor1,
            final Class<I0> inputType0,
            final Class<I1> inputType1) {

        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaJoinOperator(final JoinOperator<I0, I1, K> that) {
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

        final Function<I0, K> keyExtractor0 = this.keyDescriptor0.getImpl();
        final Function<I1, K> keyExtractor1 = this.keyDescriptor1.getImpl();

        final CardinalityEstimate cardinalityEstimate0 = operatorContext.getInputCardinality(0);
        final CardinalityEstimate cardinalityEstimate1 = operatorContext.getInputCardinality(1);

        final ExecutionLineageNode indexingExecutionLineageNode = new ExecutionLineageNode(operatorContext);
        indexingExecutionLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.join.load.indexing", javaExecutor.getConfiguration()));
        final ExecutionLineageNode probingExecutionLineageNode = new ExecutionLineageNode(operatorContext);
        probingExecutionLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.join.load.probing", javaExecutor.getConfiguration()));

        final Stream<Tuple2<I0, I1>> joinStream;
        final LinkedList<ExecutionLineageNode> executionLineageNodes = new LinkedList<>();
        final LinkedList<ChannelInstance> producedChannelInstances = new LinkedList<>();

        final boolean isMaterialize0 = cardinalityEstimate0 != null &&
                cardinalityEstimate1 != null &&
                cardinalityEstimate0.getGeometricMeanEstimate() <= cardinalityEstimate1.getGeometricMeanEstimate();

        if (isMaterialize0) {
            final int expectedNumElements = (int) cardinalityEstimate0.getGeometricMeanEstimate();
            final Map<K, Collection<I0>> probeTable = new HashMap<>(expectedNumElements);
            ((JavaChannelInstance) inputs[0]).<I0>provideStream()
                    .forEach(dataQuantum0 -> probeTable.compute(keyExtractor0.apply(dataQuantum0),
                            (key, value) -> {
                                value = value == null ? new LinkedList<>() : value;
                                value.add(dataQuantum0);
                                return value;
                            }));
            joinStream = ((JavaChannelInstance) inputs[1]).<I1>provideStream()
                    .flatMap(dataQuantum1 -> probeTable
                            .getOrDefault(keyExtractor1.apply(dataQuantum1), Collections.emptyList()).stream()
                            .map(dataQuantum0 -> new Tuple2<>(dataQuantum0, dataQuantum1)));
            indexingExecutionLineageNode.addPredecessor(inputs[0].getLineage());
            indexingExecutionLineageNode.collectAndMark(executionLineageNodes, producedChannelInstances);
            probingExecutionLineageNode.addPredecessor(inputs[1].getLineage());
        } else {
            final int expectedNumElements = cardinalityEstimate1 == null ? 1000
                    : (int) cardinalityEstimate1.getGeometricMeanEstimate();
            final Map<K, Collection<I1>> probeTable = new HashMap<>(expectedNumElements);
            ((JavaChannelInstance) inputs[1]).<I1>provideStream()
                    .forEach(dataQuantum1 -> probeTable.compute(keyExtractor1.apply(dataQuantum1),
                            (key, value) -> {
                                value = value == null ? new LinkedList<>() : value;
                                value.add(dataQuantum1);
                                return value;
                            }));
            joinStream = ((JavaChannelInstance) inputs[0]).<I0>provideStream()
                    .flatMap(dataQuantum0 -> probeTable
                            .getOrDefault(keyExtractor0.apply(dataQuantum0), Collections.emptyList()).stream()
                            .map(dataQuantum1 -> new Tuple2<>(dataQuantum0, dataQuantum1)));
            indexingExecutionLineageNode.addPredecessor(inputs[1].getLineage());
            indexingExecutionLineageNode.collectAndMark(executionLineageNodes, producedChannelInstances);
            probingExecutionLineageNode.addPredecessor(inputs[0].getLineage());
        }

        ((StreamChannel.Instance) outputs[0]).accept(joinStream);
        outputs[0].getLineage().addPredecessor(probingExecutionLineageNode);

        return new Tuple<>(executionLineageNodes, producedChannelInstances);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.java.join.load.indexing", "wayang.java.join.load.probing");
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
    protected ExecutionOperator createCopy() {
        return new JavaJoinOperator<I0, I1, K>(
                this.getKeyDescriptor0(),
                this.getKeyDescriptor1(),
                this.getInputType0().getDataUnitType().getTypeClass(),
                this.getInputType1().getDataUnitType().getTypeClass());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
