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

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import org.apache.wayang.basic.operators.PageRankOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.flink.execution.FlinkExecutor;

/**
 * Flink implementation of the {@link PageRankOperator}.
 */
public class FlinkPageRankOperator extends PageRankOperator implements FlinkExecutionOperator {
    /**
     * A map function that assigns an initial rank to all pages.
     */
    public static final class RankAssigner implements MapFunction<Long, Tuple2<Long, Double>> {
        final Tuple2<Long, Double> outPageWithRank;

        public RankAssigner(final double rank) {
            this.outPageWithRank = new Tuple2<Long, Double>(-1L, rank);
        }

        @Override
        public Tuple2<Long, Double> map(final Long page) {
            outPageWithRank.f0 = page;
            return outPageWithRank;
        }
    }

    /**
     * A reduce function that takes a sequence of edges and builds the adjacency
     * list for the vertex where the edges
     * originate. Run as a pre-processing step.
     */
    public static final class BuildOutgoingEdgeList
            implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {

        private final ArrayList<Long> neighbors = new ArrayList<Long>();

        @Override
        public void reduce(final Iterable<Tuple2<Long, Long>> values, final Collector<Tuple2<Long, Long[]>> out) {
            neighbors.clear();
            Long id = 0L;

            for (final Tuple2<Long, Long> n : values) {
                id = n.f0;
                neighbors.add(n.f1);
            }
            out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
        }
    }

    /**
     * Join function that distributes a fraction of a vertex's rank to all
     * neighbors.
     */
    public static final class JoinVertexWithEdgesMatch
            implements FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>> {

        @Override
        public void flatMap(final Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value,
                final Collector<Tuple2<Long, Double>> out) {
            final Long[] neighbors = value.f1.f1;
            final double rank = value.f0.f1;
            final double rankToDistribute = rank / ((double) neighbors.length);

            for (final Long neighbor : neighbors) {
                out.collect(new Tuple2<Long, Double>(neighbor, rankToDistribute));
            }
        }
    }

    /**
     * The function that applies the page rank dampening formula.
     */
    public static final class Dampener implements MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

        private final double dampening;
        private final double randomJump;

        public Dampener(final double dampening, final double numVertices) {
            this.dampening = dampening;
            this.randomJump = (1 - dampening) / numVertices;
        }

        @Override
        public Tuple2<Long, Double> map(final Tuple2<Long, Double> value) {
            value.f1 = (value.f1 * dampening) + randomJump;
            return value;
        }
    }

    /**
     * Filter that filters vertices where the rank difference is below a threshold.
     */
    public static final class EpsilonFilter
            implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {

        @Override
        public boolean filter(final Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) {
            return Math.abs(value.f0.f1 - value.f1.f1) > EPSILON;
        }
    }

    private static final float DAMPENING_FACTOR = 0.85f;

    private static final float EPSILON = 0.001f;

    public FlinkPageRankOperator(final Integer numIterations) {
        super(numIterations);
    }

    public FlinkPageRankOperator(final PageRankOperator pageRankOperator) {
        super(pageRankOperator);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final FlinkExecutor flinkExecutor,
            final OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final DataSetChannel.Instance input = (DataSetChannel.Instance) inputs[0];
        final DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];

        final MapFunction<org.apache.wayang.basic.data.Tuple2<Long, Long>, Tuple2<Long, Long>> mapFunction = new MapFunction<org.apache.wayang.basic.data.Tuple2<Long, Long>, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(final org.apache.wayang.basic.data.Tuple2<Long, Long> longLongTuple2)
                    throws Exception {
                return new Tuple2<>(longLongTuple2.getField0(), longLongTuple2.getField1());
            }
        };

        final DataSet<org.apache.wayang.basic.data.Tuple2<Long, Long>> dataSetInput = input.provideDataSet();

        final DataSet<Tuple2<Long, Long>> dataSetInputReal = dataSetInput.map(mapFunction);

        final FlatMapFunction<Tuple2<Long, Long>, Long> flatMapFunction = new FlatMapFunction<Tuple2<Long, Long>, Long>() {
            @Override
            public void flatMap(final Tuple2<Long, Long> longLongTuple2, final Collector<Long> collector) throws Exception {
                collector.collect(longLongTuple2.f0);
                collector.collect(longLongTuple2.f1);
            }
        };

        final DataSet<Long> pages = dataSetInputReal.flatMap(flatMapFunction).distinct();

        int numPages = 0;
        try {
            numPages = (int) pages.count();
        } catch (final Exception e) {
            e.printStackTrace();
        }

        // get input data
        final DataSet<Long> pagesInput = pages;
        final DataSet<Tuple2<Long, Long>> linksInput = dataSetInputReal;

        // assign initial rank to pages
        final DataSet<Tuple2<Long, Double>> pagesWithRanks = pagesInput.map(new RankAssigner((1.0d / numPages)));

        // build adjacency list from link input
        final DataSet<Tuple2<Long, Long[]>> adjacencyListInput = linksInput.groupBy(0)
                .reduceGroup(new BuildOutgoingEdgeList());

        // set iterative data set
        final IterativeDataSet<Tuple2<Long, Double>> iteration = pagesWithRanks.iterate(this.numIterations);

        final DataSet<Tuple2<Long, Double>> newRanks = iteration
                // join pages with outgoing edges and distribute rank
                .join(adjacencyListInput).where(0).equalTo(0).flatMap(new JoinVertexWithEdgesMatch())
                // collect and sum ranks
                .groupBy(0).aggregate(SUM, 1)
                // apply dampening factor
                .map(new Dampener(DAMPENING_FACTOR, numPages));

        final DataSet<Tuple2<Long, Double>> finalPageRanks = iteration.closeWith(
                newRanks,
                newRanks.join(iteration).where(0).equalTo(0)
                        // termination condition
                        .filter(new EpsilonFilter()));

        final DataSet<org.apache.wayang.basic.data.Tuple2<Long, Float>> dataSetOutput = finalPageRanks.map(
                new MapFunction<Tuple2<Long, Double>, org.apache.wayang.basic.data.Tuple2<Long, Float>>() {
                    @Override
                    public org.apache.wayang.basic.data.Tuple2<Long, Float> map(final Tuple2<Long, Double> longDoubleTuple2)
                            throws Exception {
                        return new org.apache.wayang.basic.data.Tuple2<Long, Float>(longDoubleTuple2.f0,
                                longDoubleTuple2.f1.floatValue());
                    }
                });

        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.flink.pagerank.load.main", "wayang.flink.pagerank.load.output");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkPageRankOperator(this.numIterations);
    }
}
