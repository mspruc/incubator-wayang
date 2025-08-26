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

package org.apache.wayang.flink.operators;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;

import org.apache.wayang.basic.operators.FlatMapOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.flink.execution.FlinkExecutionContext;
import org.apache.wayang.flink.execution.FlinkExecutor;

/**
 * Flink implementation of the {@link FlatMapOperator}.
 */
public class FlinkFlatMapOperator<I, O>
        extends FlatMapOperator<I, O>
        implements FlinkExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param functionDescriptor
     */
    public FlinkFlatMapOperator(final DataSetType<I> inputType, final DataSetType<O> outputType,
            final FlatMapDescriptor<I, O> functionDescriptor) {
        super(functionDescriptor, inputType, outputType);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkFlatMapOperator(final FlatMapOperator<I, O> that) {
        super(that);
    }

    public FlinkFlatMapOperator() {
        super((FlatMapOperator<I, O>) null);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final FlinkExecutor flinkExecutor,
            final OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final DataSetChannel.Instance input = (DataSetChannel.Instance) inputs[0];
        final DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];

        final DataSet<I> dataSetInput = input.provideDataSet();

        final DataSet<O> dataSetOutput;
        if (this.getNumBroadcastInputs() > 0) {

            final Tuple<String, DataSet> names = searchBroadcast(inputs);

            final FlinkExecutionContext fex = new FlinkExecutionContext(this, inputs, 0);

            final RichFlatMapFunction<I, O> richFunction = flinkExecutor.compiler
                    .compile((FunctionDescriptor.ExtendedSerializableFunction) this.functionDescriptor
                                    .getJavaImplementation(),
                            fex);
            fex.setRichFunction(richFunction);

            dataSetOutput = dataSetInput
                    .flatMap(richFunction)
                    .setParallelism(flinkExecutor.fee.getParallelism())
                    .returns(this.functionDescriptor.getOutputType().getTypeClass())
                    .withBroadcastSet(names.getField1(), names.getField0());

        } else {

            final FlatMapFunction<I, O> flatMapFunction = flinkExecutor.getCompiler()
                    .compile((FunctionDescriptor.SerializableFunction<I, Iterable<O>>) this.functionDescriptor
                            .getJavaImplementation());

            dataSetOutput = dataSetInput.flatMap(flatMapFunction)
                    .setParallelism(flinkExecutor.fee.getParallelism())
                    .returns(this.functionDescriptor.getOutputType().getTypeClass());
        }
        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.flink.flatmap.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(final Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator = FlinkExecutionOperator.super.createLoadProfileEstimator(
                configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.functionDescriptor, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkFlatMapOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }

    private Tuple<String, DataSet> searchBroadcast(final ChannelInstance[] inputs) {
        for (int i = 0; i < this.inputSlots.length; i++) {
            if (this.inputSlots[i].isBroadcast()) {
                final DataSetChannel.Instance dataSetChannel = (DataSetChannel.Instance) inputs[inputSlots[i].getIndex()];
                return new Tuple<>(inputSlots[i].getName(), dataSetChannel.provideDataSet());
            }
        }
        return null;
    }

}
