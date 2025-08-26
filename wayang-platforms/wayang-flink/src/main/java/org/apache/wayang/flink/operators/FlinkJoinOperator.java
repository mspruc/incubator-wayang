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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.impl.IJavaImpl;
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
import org.apache.wayang.flink.compiler.FunctionCompiler;
import org.apache.wayang.flink.execution.FlinkExecutor;

/**
 * Flink implementation of the {@link JoinOperator}.
 */
public class FlinkJoinOperator<I0 extends Serializable, I1 extends Serializable, K extends Serializable>
        extends JoinOperator<I0, I1, K>
        implements FlinkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public FlinkJoinOperator(final DataSetType<I0> inputType0,
            final DataSetType<I1> inputType1,
            final TransformationDescriptor<I0, K> keyDescriptor0,
            final TransformationDescriptor<I1, K> keyDescriptor1) {

        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    /**
     * Creates a new instance.
     */
    public FlinkJoinOperator(
            final IJavaImpl<SerializableFunction<I0, K>> keyDescriptor0,
            final IJavaImpl<SerializableFunction<I1, K>> keyDescriptor1,
            final DataSetType<I0> inputType0,
            final DataSetType<I1> inputType1) {
        super(keyDescriptor0, keyDescriptor1, inputType0.getDataUnitType().getTypeClass(),
                inputType1.getDataUnitType().getTypeClass());
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkJoinOperator(final JoinOperator<I0, I1, K> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final FlinkExecutor flinkExecutor,
            final OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final DataSetChannel.Instance input0 = (DataSetChannel.Instance) inputs[0];
        final DataSetChannel.Instance input1 = (DataSetChannel.Instance) inputs[1];
        final DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];

        final DataSet<I0> dataSetInput0 = input0.provideDataSet();
        final DataSet<I1> dataSetInput1 = input1.provideDataSet();

        final FunctionCompiler compiler = flinkExecutor.getCompiler();

        final KeySelector<I0, K> fun0 = compiler.compileKeySelector(this.keyDescriptor0);
        final KeySelector<I1, K> fun1 = compiler.compileKeySelector(this.keyDescriptor1);

        final DataSet<Tuple2<I0, I1>> dataSetOutput = dataSetInput0.join(dataSetInput1)
                .where(fun0)
                .equalTo(fun1)
                .with(
                        new JoinFunction<I0, I1, Tuple2<I0, I1>>() {
                            @Override
                            public Tuple2<I0, I1> join(final I0 inputType0,
                                    final I1 inputType1) throws Exception {
                                return new Tuple2<I0, I1>(inputType0, inputType1);
                            }
                        })
                .setParallelism(flinkExecutor.fee.getParallelism());

        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.flink.join.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(final Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator = FlinkExecutionOperator.super.createLoadProfileEstimator(
                configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor0, configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor1, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        // return Collections.singletonList(DataSetChannel.DESCRIPTOR);
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkJoinOperator<>(this.getKeyDescriptor0(),
                this.getKeyDescriptor1(),
                this.getInputType0(),
                this.getInputType1());
    }
}
