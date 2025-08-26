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

package org.apache.wayang.spark.operators;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.BroadcastChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;

/**
 * Spark implementation of the {@link ReduceByOperator}.
 */
public class SparkReduceByOperator<T, K>
        extends ReduceByOperator<T, K>
        implements SparkExecutionOperator {

    /**
     * Extracts the value from a {@link scala.Tuple2}.
     * <p>
     * <i>TODO: See, if we can somehow dodge all this conversion, which is likely to
     * happen a lot.</i>
     * </p>
     */
    private static class TupleConverter<I, K>
            implements Function<scala.Tuple2<K, I>, I> {

        @Override
        public I call(final scala.Tuple2<K, I> scalaTuple) throws Exception {
            return scalaTuple._2;
        }
    }

    /**
     * Creates a new instance.
     *
     * @param type             type of the reduce elements (i.e., type of
     *                         {@link #getInput()} and {@link #getOutput()})
     * @param keyDescriptor    describes how to extract the key from data units
     * @param reduceDescriptor describes the reduction to be performed on the
     *                         elements
     */
    public SparkReduceByOperator(final DataSetType<T> type, final TransformationDescriptor<T, K> keyDescriptor,
            final ReduceDescriptor<T> reduceDescriptor) {
        super(keyDescriptor, reduceDescriptor, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkReduceByOperator(final ReduceByOperator<T, K> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final SparkExecutor sparkExecutor,
            final OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final JavaRDD<T> inputStream = input.provideRdd();
        final PairFunction<T, K, T> keyExtractor = sparkExecutor.getCompiler()
                .compileToKeyExtractor(this.keyDescriptor);
        final Function2<T, T, T> reduceFunc = sparkExecutor.getCompiler().compile(this.reduceDescriptor, this,
                operatorContext, inputs);
        final JavaPairRDD<K, T> pairRdd = inputStream.mapToPair(keyExtractor);
        this.name(pairRdd);
        final JavaPairRDD<K, T> reducedPairRdd = pairRdd.reduceByKey(reduceFunc,
                sparkExecutor.getNumDefaultPartitions());
        this.name(reducedPairRdd);
        final JavaRDD<T> outputRdd = reducedPairRdd.map(new TupleConverter<>());
        this.name(outputRdd);

        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.reduceby.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(final Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator = SparkExecutionOperator.super.createLoadProfileEstimator(
                configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor, configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.reduceDescriptor, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        if (index == 0) {
            return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
        } else {
            return Collections.singletonList(BroadcastChannel.DESCRIPTOR);
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkReduceByOperator<>(this.getType(), this.getKeyDescriptor(), this.getReduceDescriptor());
    }
}
