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

package org.apache.wayang.basic.mapping;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.CountOperator;
import org.apache.wayang.basic.operators.DistinctOperator;
import org.apache.wayang.basic.operators.FlatMapOperator;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.PageRankOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.basic.operators.RepeatOperator;
import org.apache.wayang.core.function.ExecutionContext;
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.LoopIsolator;
import org.apache.wayang.core.plan.wayangplan.LoopSubplan;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.Subplan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.WayangCollections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This {@link Mapping} translates a {@link PageRankOperator} into a
 * {@link Subplan} of basic {@link Operator}s.
 */
public class PageRankMapping implements Mapping {

    private static final double NUM_VERTICES_PER_EDGE = 0.01d;

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(this.createTransformation());
    }

    private PlanTransformation createTransformation() {
        return new PlanTransformation(
                this.createPattern(),
                this.createReplacementFactory());
    }

    private SubplanPattern createPattern() {
        return SubplanPattern.createSingleton(new OperatorPattern<>(
                "pageRank",
                new PageRankOperator(1),
                false));
    }

    private ReplacementSubplanFactory createReplacementFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<>(this::createPageRankSubplan);
    }

    private Operator createPageRankSubplan(final PageRankOperator pageRankOperator, final int epoch) {
        final String operatorBaseName = pageRankOperator.getName() == null ? "PageRank"
                : pageRankOperator.getName();

        // TODO: We only need this MapOperator, because we cannot have a singl Subplan
        // InputSlot that maps to two
        // inner InputSlots.
        final MapOperator<Tuple2<Long, Long>, Tuple2<Long, Long>> forward = new MapOperator<>(
                t -> t, ReflectionUtils.specify(Tuple2.class), ReflectionUtils.specify(Tuple2.class));
        forward.at(epoch);
        forward.setName(String.format("%s (forward)", operatorBaseName));

        // Find all vertices.
        final FlatMapOperator<Tuple2<Long, Long>, Long> vertexExtractor = new FlatMapOperator<>(
                new FlatMapDescriptor<>(
                        (FunctionDescriptor.SerializableFunction<Tuple2<Long, Long>, Iterable<Long>>) edge -> {
                            final List<Long> out = new ArrayList<>(2);
                            out.add(edge.getField0());
                            out.add(edge.getField1());
                            return out;
                        },
                        ReflectionUtils.specify(Tuple2.class), Long.class,
                        ProbabilisticDoubleInterval.ofExactly(2)));
        vertexExtractor.at(epoch);
        vertexExtractor.setName(String.format("%s (extract vertices)", operatorBaseName));
        forward.connectTo(0, vertexExtractor, 0);

        // Get the distinct vertices.
        final DistinctOperator<Long> vertexDistincter = new DistinctOperator<>(Long.class);
        vertexDistincter.at(epoch);
        vertexDistincter.setName(String.format("%s (distinct vertices)", operatorBaseName));
        vertexDistincter.setCardinalityEstimator(0, new DefaultCardinalityEstimator(
                0.5d, 1, false, longs -> Math.round(longs[0] * NUM_VERTICES_PER_EDGE / 2)));
        vertexExtractor.connectTo(0, vertexDistincter, 0);

        // Count the vertices.
        final CountOperator<Long> vertexCounter = new CountOperator<>(Long.class);
        vertexCounter.at(epoch);
        vertexCounter.setName(String.format("%s (count vertices)", operatorBaseName));
        vertexDistincter.connectTo(0, vertexCounter, 0);

        // Create the adjancencies.
        final MapOperator<Tuple2<Long, Long>, Tuple2<Long, long[]>> adjacencyPreparator = new MapOperator<>(
                t -> new Tuple2<>(t.getField0(), new long[] { t.getField1() }),
                ReflectionUtils.specify(Tuple2.class),
                ReflectionUtils.specify(Tuple2.class));
        adjacencyPreparator.at(epoch);
        adjacencyPreparator.setName(String.format("%s (prepare adjacencies)", operatorBaseName));
        forward.connectTo(0, adjacencyPreparator, 0);

        final ReduceByOperator<Tuple2<Long, long[]>, Long> adjacencyCreator = new ReduceByOperator<>(
                Tuple2::getField0,
                (t1, t2) -> {
                    // NB: We don't care about duplicates because they should influence the
                    // PageRanks.
                    // That being said, in some cases there are more efficient implementations of
                    // bags.
                    final long[] targetVertices = new long[t1.getField1().length + t2.getField1().length];
                    System.arraycopy(t1.getField1(), 0, targetVertices, 0, t1.getField1().length);
                    System.arraycopy(t2.getField1(), 0, targetVertices, t1.getField1().length,
                            t2.getField1().length);
                    return new Tuple2<>(t1.getField0(), targetVertices);
                },
                ReflectionUtils.specify(Long.class),
                ReflectionUtils.specify(Tuple2.class));
        adjacencyCreator.at(epoch);
        adjacencyCreator.setName(String.format("%s (create adjacencies)", operatorBaseName));
        adjacencyCreator.setCardinalityEstimator(0, new DefaultCardinalityEstimator(
                0.5d, 1, false, longs -> Math.round(longs[0] * NUM_VERTICES_PER_EDGE)));
        adjacencyPreparator.connectTo(0, adjacencyCreator, 0);

        // Create the initial page ranks.
        final MapOperator<Long, Tuple2<Long, Float>> initializeRanks = new MapOperator<>(
                new RankInitializer(),
                Long.class, ReflectionUtils.specify(Tuple2.class));
        initializeRanks.at(epoch);
        initializeRanks.setName(String.format("%s (initialize ranks)", operatorBaseName));
        vertexDistincter.connectTo(0, initializeRanks, 0);
        vertexCounter.broadcastTo(0, initializeRanks, "numVertices");

        // Send the initial page ranks into the loop.
        final RepeatOperator<Tuple2<Long, long[]>> loopHead = new RepeatOperator<>(
                pageRankOperator.getNumIterations(), ReflectionUtils.specify(Tuple2.class));
        loopHead.at(epoch);
        loopHead.setName(String.format("%s (loop head)", operatorBaseName));
        loopHead.initialize(initializeRanks, 0);

        // Join adjacencies and current ranks.
        final JoinOperator<Tuple2<Long, long[]>, Tuple2<Long, Float>, Long> rankJoin = new JoinOperator<>(
                Tuple2::getField0,
                Tuple2::getField0,
                ReflectionUtils.specify(Tuple2.class),
                ReflectionUtils.specify(Tuple2.class),
                Long.class);
        rankJoin.at(epoch);
        rankJoin.setName(String.format("%s (join adjacencies and ranks)", operatorBaseName));
        rankJoin.setCardinalityEstimator(0, new DefaultCardinalityEstimator(
                .99d, 2, false, longs -> longs[0]));
        adjacencyCreator.connectTo(0, rankJoin, 0);
        loopHead.connectTo(RepeatOperator.ITERATION_OUTPUT_INDEX, rankJoin, 1);

        // Create the new partial ranks.
        final FlatMapOperator<Tuple2<Tuple2<Long, long[]>, Tuple2<Long, Float>>, Tuple2<Long, Float>> partialRankCreator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(
                        adjacencyAndRank -> {
                            final Long sourceVertex = adjacencyAndRank.getField0().getField0();
                            final long[] targetVertices = adjacencyAndRank.getField0().getField1();
                            final float baseRank = adjacencyAndRank.getField1().getField1();
                            final Float partialRank = baseRank / targetVertices.length;
                            final Collection<Tuple2<Long, Float>> partialRanks = new ArrayList<>(
                                    targetVertices.length + 1);
                            for (final long targetVertex : targetVertices) {
                                partialRanks.add(new Tuple2<>(targetVertex,
                                        partialRank));
                            }
                            // Add a surrogate partial rank to avoid losing unreferenced
                            // vertices.
                            partialRanks.add(new Tuple2<>(sourceVertex, 0f));
                            return partialRanks;
                        },
                        ReflectionUtils.specify(Tuple2.class),
                        ReflectionUtils.specify(Tuple2.class),
                        ProbabilisticDoubleInterval.ofExactly(1d / NUM_VERTICES_PER_EDGE)));
        partialRankCreator.at(epoch);
        partialRankCreator.setName(String.format("%s (create partial ranks)", operatorBaseName));
        rankJoin.connectTo(0, partialRankCreator, 0);

        // Sum the partial ranks.
        final ReduceByOperator<Tuple2<Long, Float>, Long> sumPartialRanks = new ReduceByOperator<>(
                Tuple2::getField0,
                (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1()),
                Long.class,
                ReflectionUtils.specify(Tuple2.class));
        sumPartialRanks.at(epoch);
        sumPartialRanks.setName(String.format("%s (sum partial ranks)", operatorBaseName));
        sumPartialRanks.setCardinalityEstimator(0, new DefaultCardinalityEstimator(
                0.5d, 1, false, longs -> Math.round(longs[0] * NUM_VERTICES_PER_EDGE)));
        partialRankCreator.connectTo(0, sumPartialRanks, 0);

        // Apply the damping factor.
        final MapOperator<Tuple2<Long, Float>, Tuple2<Long, Float>> damping = new MapOperator<>(
                new ApplyDamping(pageRankOperator.getDampingFactor()),
                ReflectionUtils.specify(Tuple2.class),
                ReflectionUtils.specify(Tuple2.class));
        damping.at(epoch);
        damping.setName(String.format("%s (damping)", operatorBaseName));
        sumPartialRanks.connectTo(0, damping, 0);
        vertexCounter.broadcastTo(0, damping, "numVertices");
        loopHead.endIteration(damping, 0);

        final LoopSubplan loopSubplan = LoopIsolator.isolate(loopHead);
        loopSubplan.at(epoch);

        return Subplan.wrap(
                Collections.singletonList(forward.getInput()),
                Collections.singletonList(loopSubplan.getOutput(0)),
                null).at(epoch);
    }

    /**
     * Creates intial page ranks.
     */
    public static class RankInitializer
            implements FunctionDescriptor.ExtendedSerializableFunction<Long, Tuple2<Long, Float>> {

        private Float initialRank;

        @Override
        public void open(final ExecutionContext ctx) {
            final long numVertices = WayangCollections.getSingle(ctx.getBroadcast("numVertices"));
            this.initialRank = 1f / numVertices;
        }

        @Override
        public Tuple2<Long, Float> apply(final Long vertexId) {
            return new Tuple2<>(vertexId, this.initialRank);
        }
    }

    /**
     * Applies damping to page ranks.
     */
    private static class ApplyDamping implements
            FunctionDescriptor.ExtendedSerializableFunction<Tuple2<Long, Float>, Tuple2<Long, Float>> {

        private final float dampingFactor;

        private float minRank;

        private ApplyDamping(final float dampingFactor) {
            this.dampingFactor = dampingFactor;
        }

        @Override
        public void open(final ExecutionContext ctx) {
            final long numVertices = WayangCollections.getSingle(ctx.getBroadcast("numVertices"));
            this.minRank = (1 - this.dampingFactor) / numVertices;
        }

        @Override
        public Tuple2<Long, Float> apply(final Tuple2<Long, Float> rank) {
            return new Tuple2<>(
                    rank.getField0(),
                    this.minRank + this.dampingFactor * rank.getField1());
        }
    }
}
