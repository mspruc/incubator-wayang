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

package org.apache.wayang.core.platform.lineage;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.util.Tuple;

/**
 * A node wraps a {@link ChannelInstance} and keeps track of predecessor nodes.
 */
public abstract class LazyExecutionLineageNode implements Serializable {

    /**
     * Callback interface for traversals of {@link LazyExecutionLineageNode}s,
     * thereby accumulating the callback return values.
     *
     * @param <T> type of the accumulator
     */
    public interface Aggregator<T> {

        /**
         * Visit an {@link ChannelLineageNode}.
         *
         * @param accumulator current accumulator value
         * @param node        the visited {@link ChannelLineageNode}
         * @return the new accumulator value
         */
        T aggregate(T accumulator, ChannelLineageNode node);

        /**
         * Visit an {@link ExecutionLineageNode}.
         *
         * @param accumulator current accumulator value
         * @param node        the visited {@link ExecutionLineageNode}
         * @return the new accumulator value
         */
        T aggregate(T accumulator, ExecutionLineageNode node);

    }

    /**
     * {@link Aggregator} implementation that collects all visited
     * {@link LazyExecutionLineageNode} contents.
     */
    public static class CollectingAggregator
            implements Aggregator<Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>>> {

        @Override
        public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> aggregate(
                final Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> accumulator,
                final ChannelLineageNode node) {
            accumulator.getField1().add(node.getChannelInstance());
            return accumulator;
        }

        @Override
        public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> aggregate(
                final Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> accumulator,
                final ExecutionLineageNode node) {
            accumulator.getField0().add(node);
            return accumulator;
        }
    }

    /**
     * Set all of the {@code inputs} as predecessors of the {@code operatorContext}
     * each of the {@code outputs}.
     *
     * @param inputs               input {@link ChannelInstance}s
     * @param executionLineageNode in-between {@link ExecutionLineageNode}
     * @param outputs              output {@link ChannelInstance}s
     * @see #addPredecessor(LazyExecutionLineageNode)
     */
    public static void connectAll(final ChannelInstance[] inputs,
            final ExecutionLineageNode executionLineageNode,
            final ChannelInstance[] outputs) {
        for (final ChannelInstance input : inputs) {
            if (input != null)
                executionLineageNode.addPredecessor(input.getLineage());
        }
        for (final ChannelInstance output : outputs) {
            if (output != null)
                output.getLineage().addPredecessor(executionLineageNode);
        }
    }

    /**
     * Instances that need to be executed before this instance.
     */
    private final Collection<LazyExecutionLineageNode> predecessors = new LinkedList<>();

    /**
     * Pinned down {@link ChannelInstance}s that must not be disposed before this
     * instance has been marked as
     * executed.
     */
    private final Collection<ChannelInstance> pinnedDownChannelInstances = new LinkedList<>();

    private boolean isExecuted = false;

    /**
     * Adds a predecessor.
     *
     * @param predecessor the predecessor
     */
    public void addPredecessor(final LazyExecutionLineageNode predecessor) {
        assert !this.predecessors.contains(predecessor)
                : String.format("Lineage predecessor %s is already present.", predecessor);
        this.predecessors.add(predecessor);

        // TODO: Pinning the input ChannelInstances down like this is not very elegant.
        // A better solution would be to incorporate all LazyExecutionLineageNodes into
        // the
        // reference counting scheme. However, this would imply considerable effort to
        // get it right.
        if (!this.isExecuted && predecessor instanceof final ChannelLineageNode channelLineageNode) {
            final ChannelInstance channelInstance = channelLineageNode.getChannelInstance();
            this.pinnedDownChannelInstances.add(channelInstance);
            channelInstance.noteObtainedReference();
        }
    }

    /**
     * Traverse this instance and all its predecessors unless they are marked as
     * executed.
     *
     * @param accumulator state that is maintained over the traversal
     * @param aggregator  visits the traversed instances
     * @param isMark      whether traversed instances should be marked
     * @param <T>
     * @return the {@code accumulator} in its final state
     */
    public <T> T traverse(T accumulator, final Aggregator<T> aggregator, final boolean isMark) {
        if (!this.isExecuted) {
            for (final Iterator<LazyExecutionLineageNode> i = this.predecessors.iterator(); i.hasNext();) {
                final LazyExecutionLineageNode predecessor = i.next();
                accumulator = predecessor.traverse(accumulator, aggregator, isMark);
                if (predecessor.isExecuted) {
                    i.remove();
                }
            }
            accumulator = this.accept(accumulator, aggregator);
            if (isMark)
                this.markAsExecuted();
        }
        return accumulator;
    }

    public <T> T traverseAndMark(final T accumulator, final Aggregator<T> aggregator) {
        return this.traverse(accumulator, aggregator, true);
    }

    public <T> T traverse(final T accumulator, final Aggregator<T> aggregator) {
        return this.traverse(accumulator, aggregator, false);
    }

    /**
     * Collect and mark all unmarked {@link ExecutionLineageNode}s in this instance.
     *
     * @return the collected {@link ExecutionLineageNode}s and produced
     *         {@link ChannelInstance}s
     */
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> collectAndMark() {
        return this.collectAndMark(new LinkedList<>(), new LinkedList<>());
    }

    /**
     * Collect and mark all unmarked {@link LazyExecutionLineageNode}s in this
     * instance.
     *
     * @param executionLineageCollector collects the unmarked
     *                                  {@link ExecutionLineageNode}
     * @param channelInstanceCollector  collects the {@link ChannelInstance} in the
     *                                  unmarked {@link LazyExecutionLineageNode}s
     * @return the two collectors
     */
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> collectAndMark(
            final Collection<ExecutionLineageNode> executionLineageCollector,
            final Collection<ChannelInstance> channelInstanceCollector) {
        return this.traverseAndMark(
                new Tuple<>(executionLineageCollector, channelInstanceCollector),
                new CollectingAggregator());
    }

    protected abstract <T> T accept(T accumulator, Aggregator<T> aggregator);

    /**
     * Mark that this instance should not be traversed any more.
     */
    protected void markAsExecuted() {
        LogManager.getLogger(this.getClass()).debug("Marking {} as executed.", this);
        this.isExecuted = true;

        // Free pinned down ChannelInstances.
        for (final ChannelInstance channelInstance : this.pinnedDownChannelInstances) {
            channelInstance.noteDiscardedReference(true);
        }
        this.pinnedDownChannelInstances.clear();
    }

}
