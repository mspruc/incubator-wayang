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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.CartesianOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
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
 * Java implementation of the {@link CartesianOperator}.
 */
public class JavaCartesianOperator<I0 extends Serializable, I1 extends Serializable>
        extends CartesianOperator<I0, I1>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     */
    public JavaCartesianOperator(final DataSetType<I0> inputType0, final DataSetType<I1> inputType1) {
        super(inputType0, inputType1);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaCartesianOperator(final CartesianOperator<I0, I1> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final JavaExecutor javaExecutor,
            final OptimizationContext.OperatorContext operatorContext) {
        if (inputs.length != 2) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final Collection<ExecutionLineageNode> executionLineageNodes = new LinkedList<>();
        final Collection<ChannelInstance> producedChannelInstances = new LinkedList<>();
        final ExecutionLineageNode probingExecutionLineageNode = new ExecutionLineageNode(operatorContext);
        probingExecutionLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.cartesian.load.probing", javaExecutor.getConfiguration()));
        final ExecutionLineageNode indexingExecutionLineageNode = new ExecutionLineageNode(operatorContext);
        indexingExecutionLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.cartesian.load.indexing", javaExecutor.getConfiguration()));

        final StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];
        final ChannelInstance materializedInput;
        final ChannelInstance probingInput;

        if (inputs[0] instanceof final CollectionChannel.Instance collectionChannelInstance) {
            final Collection<I0> collection = collectionChannelInstance.provideCollection();
            final Stream<I1> stream = ((JavaChannelInstance) inputs[1]).provideStream();
            output.<Tuple2<I0, I1>>accept(
                    stream.flatMap(e1 -> collection.stream().map(
                            e0 -> new Tuple2<>(e0, e1))));
            materializedInput = inputs[0];
            probingInput = inputs[1];
            probingExecutionLineageNode.addPredecessor(materializedInput.getLineage());

        } else if (inputs[1] instanceof final CollectionChannel.Instance collectionChannelInstance) {
            final Stream<I0> stream = ((JavaChannelInstance) inputs[0]).provideStream();
            final Collection<I1> collection = collectionChannelInstance.provideCollection();
            output.<Tuple2<I0, I1>>accept(
                    stream.flatMap(e0 -> collection.stream().map(e1 -> new Tuple2<>(e0, e1))));
            materializedInput = inputs[1];
            probingInput = inputs[0];
            probingExecutionLineageNode.addPredecessor(materializedInput.getLineage());

        } else if (operatorContext.getInputCardinality(0).getGeometricMeanEstimate() <= operatorContext
                .getInputCardinality(1).getGeometricMeanEstimate()) {
            // Fallback: Materialize one side.
            final Collection<I0> collection = ((JavaChannelInstance) inputs[0]).<I0>provideStream().toList();
            final Stream<I1> stream = ((JavaChannelInstance) inputs[1]).provideStream();
            output.<Tuple2<I0, I1>>accept(
                    stream.flatMap(e1 -> collection.stream().map(
                            e0 -> new Tuple2<>(e0, e1))));
            materializedInput = inputs[0];
            probingInput = inputs[1];
            indexingExecutionLineageNode.addPredecessor(materializedInput.getLineage());
            indexingExecutionLineageNode.collectAndMark(executionLineageNodes, producedChannelInstances);
        } else {
            final Collection<I1> collection = ((JavaChannelInstance) inputs[1]).<I1>provideStream().toList();
            final Stream<I0> stream = ((JavaChannelInstance) inputs[0]).provideStream();
            output.<Tuple2<I0, I1>>accept(
                    stream.flatMap(e0 -> collection.stream().map(
                            e1 -> new Tuple2<>(e0, e1))));
            materializedInput = inputs[1];
            probingInput = inputs[0];
            indexingExecutionLineageNode.addPredecessor(materializedInput.getLineage());
            indexingExecutionLineageNode.collectAndMark(executionLineageNodes, producedChannelInstances);
        }

        probingExecutionLineageNode.addPredecessor(probingInput.getLineage());
        output.getLineage().addPredecessor(probingExecutionLineageNode);
        return new Tuple<>(executionLineageNodes, producedChannelInstances);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.java.cartesian.load.indexing", "wayang.java.cartesian.load.probing");
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

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaCartesianOperator<>(this.getInputType0(), this.getInputType1());
    }
}
