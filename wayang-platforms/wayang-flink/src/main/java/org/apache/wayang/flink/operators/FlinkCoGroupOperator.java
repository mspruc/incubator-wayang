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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.CoGroupOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.flink.compiler.FunctionCompiler;
import org.apache.wayang.flink.execution.FlinkExecutor;

/**
 * Flink implementation of the {@link CoGroupOperator}.
 */
public class FlinkCoGroupOperator<I0 extends Serializable, I1 extends Serializable, K extends Serializable>
        extends CoGroupOperator<I0, I1, K>
        implements FlinkExecutionOperator {
    /**
     * @see CoGroupOperator#CoGroupOperator(FunctionDescriptor.SerializableFunction,
     *      FunctionDescriptor.SerializableFunction, Class, Class, Class)
     */
    public FlinkCoGroupOperator(final FunctionDescriptor.SerializableFunction<I0, K> keyExtractor0,
            final FunctionDescriptor.SerializableFunction<I1, K> keyExtractor1,
            final Class<I0> input0Class,
            final Class<I1> input1Class,
            final Class<K> keyClass) {
        super(keyExtractor0, keyExtractor1, input0Class, input1Class, keyClass);
    }

    /**
     * @see CoGroupOperator#CoGroupOperator(TransformationDescriptor,
     *      TransformationDescriptor)
     */
    public FlinkCoGroupOperator(final TransformationDescriptor<I0, K> keyDescriptor0,
            final TransformationDescriptor<I1, K> keyDescriptor1) {
        super(keyDescriptor0, keyDescriptor1);
    }

    /**
     * @see CoGroupOperator#CoGroupOperator(TransformationDescriptor,
     *      TransformationDescriptor, DataSetType, DataSetType)
     */
    public FlinkCoGroupOperator(final TransformationDescriptor<I0, K> keyDescriptor0,
            final TransformationDescriptor<I1, K> keyDescriptor1,
            final DataSetType<I0> inputType0,
            final DataSetType<I1> inputType1) {
        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    /**
     * @see CoGroupOperator#CoGroupOperator(CoGroupOperator)
     */
    public FlinkCoGroupOperator(final CoGroupOperator<I0, I1, K> that) {
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

        final DataSet<I0> datasetInput0 = input0.provideDataSet();
        final DataSet<I1> datasetInput1 = input1.provideDataSet();

        final FunctionCompiler compiler = flinkExecutor.getCompiler();

        final KeySelector<I0, K> function0 = compiler.compileKeySelector(this.keyDescriptor0);
        final KeySelector<I1, K> function1 = compiler.compileKeySelector(this.keyDescriptor1);

        final DataSet<Tuple2<ArrayList<I0>, ArrayList<I1>>> datasetOutput = datasetInput0.coGroup(datasetInput1)
                .where(function0)
                .equalTo(function1)
                .with(
                        new CoGroupFunction<I0, I1, Tuple2<ArrayList<I0>, ArrayList<I1>>>() {
                            @Override
                            public void coGroup(
                                    final Iterable<I0> iterable,
                                    final Iterable<I1> iterable1,
                                    final Collector<Tuple2<ArrayList<I0>, ArrayList<I1>>> collector) {
                                final ArrayList<I0> list0 = new ArrayList<>();
                                final ArrayList<I1> list1 = new ArrayList<>();
                                iterable.forEach(list0::add);
                                iterable1.forEach(list1::add);
                                collector.collect(new Tuple2<>(list0, list1));
                            }
                        })
                .setParallelism(flinkExecutor.fee.getParallelism())
                .returns(ReflectionUtils.specify(Tuple2.class));

        output.accept(datasetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    public String getLoadProfileEstimatorConfigurationK() {
        return "wayang.flink.cogroup.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkCoGroupOperator<>(this);
    }

}
