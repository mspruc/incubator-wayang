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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.wayang.basic.operators.CoGroupOperator;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.compiler.FunctionCompiler;
import org.apache.wayang.spark.execution.SparkExecutor;

/**
 * Spark implementation of the {@link JoinOperator}.
 */
public class SparkCoGroupOperator<I0 extends Serializable, I1 extends Serializable, K extends Serializable>
        extends CoGroupOperator<I0, I1, K> implements SparkExecutionOperator {

    /**
     * Converts the output of {@link JavaPairRDD#cogroup(JavaPairRDD, int)} to what
     * Wayang expects.
     * <p>
     * <i>TODO: See, if we can somehow dodge all this conversion, which is likely to
     * happen a lot.</i>
     * </p>
     */
    private static class TupleConverter<I0, I1, K> implements
            Function<scala.Tuple2<K, scala.Tuple2<Iterable<I0>, Iterable<I1>>>, Tuple<Iterable<I0>, Iterable<I1>>> {

        @Override
        public Tuple<Iterable<I0>, Iterable<I1>> call(
                final scala.Tuple2<K, scala.Tuple2<Iterable<I0>, Iterable<I1>>> in) throws Exception {
            return new Tuple<>(in._2._1, in._2._2);
        }
    }

    /**
     * @see CoGroupOperator#CoGroupOperator(FunctionDescriptor.SerializableFunction,
     *      FunctionDescriptor.SerializableFunction, Class, Class, Class)
     */
    public SparkCoGroupOperator(final FunctionDescriptor.SerializableFunction<I0, K> keyExtractor0,
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
    public SparkCoGroupOperator(final TransformationDescriptor<I0, K> keyDescriptor0,
            final TransformationDescriptor<I1, K> keyDescriptor1) {
        super(keyDescriptor0, keyDescriptor1);
    }

    /**
     * @see CoGroupOperator#CoGroupOperator(TransformationDescriptor,
     *      TransformationDescriptor, DataSetType, DataSetType)
     */
    public SparkCoGroupOperator(final TransformationDescriptor<I0, K> keyDescriptor0,
            final TransformationDescriptor<I1, K> keyDescriptor1,
            final DataSetType<I0> inputType0,
            final DataSetType<I1> inputType1) {
        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    /**
     * @see CoGroupOperator#CoGroupOperator(CoGroupOperator)
     */
    public SparkCoGroupOperator(final CoGroupOperator<I0, I1, K> that) {
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

        final RddChannel.Instance input0 = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance input1 = (RddChannel.Instance) inputs[1];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final JavaRDD<I0> inputRdd0 = input0.provideRdd();
        final JavaRDD<I1> inputRdd1 = input1.provideRdd();

        final FunctionCompiler compiler = sparkExecutor.getCompiler();
        final PairFunction<I0, K, I0> keyExtractor0 = compiler.compileToKeyExtractor(this.keyDescriptor0);
        final PairFunction<I1, K, I1> keyExtractor1 = compiler.compileToKeyExtractor(this.keyDescriptor1);
        final JavaPairRDD<K, I0> pairRdd0 = inputRdd0.mapToPair(keyExtractor0);
        final JavaPairRDD<K, I1> pairRdd1 = inputRdd1.mapToPair(keyExtractor1);

        final JavaPairRDD<K, scala.Tuple2<Iterable<I0>, Iterable<I1>>> outputPair = pairRdd0.cogroup(pairRdd1,
                sparkExecutor.getNumDefaultPartitions());
        this.name(outputPair);

        // Map the output to what Wayang expects.
        final JavaRDD<Tuple<Iterable<I0>, Iterable<I1>>> outputRdd = outputPair.map(new TupleConverter<>());
        this.name(outputRdd);

        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.cogroup.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
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
        return new SparkCoGroupOperator<>(this);
    }
}
