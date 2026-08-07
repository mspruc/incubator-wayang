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

package org.apache.wayang.java.channels;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.AbstractChannelInstance;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.Executor;

/**
 * Compresses the incoming data via using the constant encoding schema. 
 * Works much better if sorted first. Uses Equals as comparator.
 */
public class StreamConstantEncodingChannel extends Channel {
    /**
     * {@link JavaChannelInstance} implementation for {@link StreamChannel}s.
     */
    public class Instance extends AbstractChannelInstance implements JavaChannelInstance {
        /*
         * amount of items to be compressed in the chunk
         */
        private static final int CHUNK_SIZE = 1024; // TODO: make configurable

        private Stream<?> stream;

        private long cardinality = 0;

        /**
         * flattens Stream<Stream<?>> into Stream<EncodedValue> potentially compressing values in a chunk
        */
        private Stream<?> flatten(final Stream<Stream<?>> chunkedStream) {
            return chunkedStream.flatMap(EncodedValue::of_chunk);
        }

        /**
         * splits a stream into sized chunks
         */
        private Stream<Stream<?>> chunk(final Stream<?> stream, final int size) {
            final Iterator<?> iterator = stream.iterator();

            final Iterator<Stream<?>> chunkedStreamIterator = new Iterator<Stream<?>>() {
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                public Stream<?> next() {
                    return Stream
                            // generate a stream from the previous stream's iterator that streams over .limit(size) sized chunks
                            .generate(() -> iterator.hasNext() 
                                ? Optional.ofNullable(iterator.next()) 
                                : Optional.empty())
                            .limit(size)
                            // filter out padded optionals on final chunk if less than chunk_size, and unwrap optional
                            .takeWhile(opt -> opt.isPresent())
                            .map(opt -> opt.get());
                }
            };

            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(chunkedStreamIterator, Spliterator.ORDERED),
                    false);
        }

        public Instance(final Executor executor, final OptimizationContext.OperatorContext producerOperatorContext,
                final int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        public void accept(final Stream<?> stream) {
            assert this.stream == null;
            final Stream<Stream<?>> chunkedStream = chunk(stream, CHUNK_SIZE);
            final Stream<?> flattenedStream = flatten(chunkedStream);

            this.stream = flattenedStream;

            if (this.isMarkedForInstrumentation()) {
                this.stream.peek(dataQuantum -> this.cardinality += 1);
            }
        }

        public void accept(final Collection<?> collection) {
            this.accept(collection.stream());
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Stream<T> provideStream() {
            return (Stream<T>) this.stream;
        }

        @Override
        public Channel getChannel() {
            return StreamConstantEncodingChannel.this;
        }

        @Override
        public OptionalLong getMeasuredCardinality() {
            return this.cardinality == 0 ? super.getMeasuredCardinality() : OptionalLong.of(this.cardinality);
        }

        @Override
        protected void doDispose() throws WayangException {
            this.stream = null;
        }
    }

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(StreamChannel.class, false, false);

    public StreamConstantEncodingChannel(final ChannelDescriptor descriptor, final OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
        assert descriptor == DESCRIPTOR;
    }

    private StreamConstantEncodingChannel(final StreamConstantEncodingChannel parent) {
        super(parent);
    }

    @Override
    public Channel copy() {
        return new StreamConstantEncodingChannel(this);
    }

    @Override
    public Instance createInstance(final Executor executor, final OperatorContext producerOperatorContext,
            final int producerOutputIndex) {
        return new Instance(executor, producerOperatorContext, producerOutputIndex);
    }
}

record EncodedValue<T>(short amount, T value) {
    /**
     * counts recurring items and compresses if all items in a chunk match
     */
    static <T> Stream<EncodedValue<?>> of_chunk(Stream<T> chunk){
        final Map<T, Integer> counts = chunk.collect(Collectors.groupingByConcurrent(
            Function.identity(),
            Collectors.summingInt(e -> 1)
        ));
        
        return counts.entrySet().stream()
            .map(e -> new EncodedValue<>(e.getValue().shortValue(), e.getKey()));
    }
};

/**
 * A wrapper for stream that gracefully handles stream operations 
 * such that the user will never have to worry about whether the previous input was 
 * compressed or chunked or not.
 */
final class EncodedStream<T> implements Stream<Object> {

    final Stream stream;

    public EncodedStream(final Stream stream) {
        this.stream = stream;
    }

    @Override
    public void close() {
        stream.close();
    }

    @Override
    public boolean isParallel() {
        throw new UnsupportedOperationException("Unimplemented method 'isParallel'");
    }

    @Override
    public Iterator<Object> iterator() {
        throw new UnsupportedOperationException("Unimplemented method 'iterator'");
    }

    @Override
    public Stream<Object> onClose(Runnable closeHandler) {
        throw new UnsupportedOperationException("Unimplemented method 'onClose'");
    }

    @Override
    public Stream<Object> parallel() {
        throw new UnsupportedOperationException("Unimplemented method 'parallel'");
    }

    @Override
    public Stream<Object> sequential() {
        throw new UnsupportedOperationException("Unimplemented method 'sequential'");
    }

    @Override
    public Spliterator<Object> spliterator() {
        throw new UnsupportedOperationException("Unimplemented method 'spliterator'");
    }

    @Override
    public Stream<Object> unordered() {
        throw new UnsupportedOperationException("Unimplemented method 'unordered'");
    }

    @Override
    public boolean allMatch(Predicate<? super Object> predicate) {
        throw new UnsupportedOperationException("Unimplemented method 'allMatch'");
    }

    @Override
    public boolean anyMatch(Predicate<? super Object> predicate) {
        throw new UnsupportedOperationException("Unimplemented method 'anyMatch'");
    }

    @Override
    public <R, A> R collect(Collector<? super Object, A, R> collector) {
        throw new UnsupportedOperationException("Unimplemented method 'collect'");
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super Object> accumulator, BiConsumer<R, R> combiner) {
        throw new UnsupportedOperationException("Unimplemented method 'collect'");
    }

    @Override
    public long count() {
        throw new UnsupportedOperationException("Unimplemented method 'count'");
    }

    @Override
    public Stream<Object> distinct() {
        throw new UnsupportedOperationException("Unimplemented method 'distinct'");
    }

    @Override
    public Stream<Object> filter(Predicate<? super Object> predicate) {
        throw new UnsupportedOperationException("Unimplemented method 'filter'");
    }

    @Override
    public Optional<Object> findAny() {
        throw new UnsupportedOperationException("Unimplemented method 'findAny'");
    }

    @Override
    public Optional<Object> findFirst() {
        throw new UnsupportedOperationException("Unimplemented method 'findFirst'");
    }

    @Override
    public <R> Stream<R> flatMap(Function<? super Object, ? extends Stream<? extends R>> mapper) {
        throw new UnsupportedOperationException("Unimplemented method 'flatMap'");
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super Object, ? extends DoubleStream> mapper) {
        throw new UnsupportedOperationException("Unimplemented method 'flatMapToDouble'");
    }

    @Override
    public IntStream flatMapToInt(Function<? super Object, ? extends IntStream> mapper) {
        throw new UnsupportedOperationException("Unimplemented method 'flatMapToInt'");
    }

    @Override
    public LongStream flatMapToLong(Function<? super Object, ? extends LongStream> mapper) {
        throw new UnsupportedOperationException("Unimplemented method 'flatMapToLong'");
    }

    @Override
    public void forEach(Consumer<? super Object> action) {
        throw new UnsupportedOperationException("Unimplemented method 'forEach'");
    }

    @Override
    public void forEachOrdered(Consumer<? super Object> action) {
        throw new UnsupportedOperationException("Unimplemented method 'forEachOrdered'");
    }

    @Override
    public Stream<Object> limit(long maxSize) {
        throw new UnsupportedOperationException("Unimplemented method 'limit'");
    }

    @Override
    public <R> Stream<R> map(Function<? super Object, ? extends R> mapper) {
        /*
         * return new EncodedStream<R>(test.map( obj -> { var value = obj instanceof
         * EncodedValue ? ((EncodedValue<?>) obj).value() : obj; R mapped =
         * mapper.apply(value); return mapped; } ));
         */

        throw new UnsupportedOperationException("Unimplemented method 'map'");
    }

    @Override
    public DoubleStream mapToDouble(ToDoubleFunction<? super Object> mapper) {
        throw new UnsupportedOperationException("Unimplemented method 'mapToDouble'");
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super Object> mapper) {
        throw new UnsupportedOperationException("Unimplemented method 'mapToInt'");
    }

    @Override
    public LongStream mapToLong(ToLongFunction<? super Object> mapper) {
        throw new UnsupportedOperationException("Unimplemented method 'mapToLong'");
    }

    @Override
    public Optional<Object> max(Comparator<? super Object> comparator) {
        throw new UnsupportedOperationException("Unimplemented method 'max'");
    }

    @Override
    public Optional<Object> min(Comparator<? super Object> comparator) {
        throw new UnsupportedOperationException("Unimplemented method 'min'");
    }

    @Override
    public boolean noneMatch(Predicate<? super Object> predicate) {
        throw new UnsupportedOperationException("Unimplemented method 'noneMatch'");
    }

    @Override
    public Stream<Object> peek(Consumer<? super Object> action) {
        throw new UnsupportedOperationException("Unimplemented method 'peek'");
    }

    @Override
    public Optional<Object> reduce(BinaryOperator<Object> accumulator) {
        throw new UnsupportedOperationException("Unimplemented method 'reduce'");
    }

    @Override
    public Object reduce(Object arg0, BinaryOperator<Object> arg1) {
        throw new UnsupportedOperationException("Unimplemented method 'reduce'");
    }

    @Override
    public <U> U reduce(U arg0, BiFunction<U, ? super Object, U> arg1, BinaryOperator<U> arg2) {
        throw new UnsupportedOperationException("Unimplemented method 'reduce'");
    }

    @Override
    public Stream<Object> skip(long n) {
        throw new UnsupportedOperationException("Unimplemented method 'skip'");
    }

    @Override
    public Stream<Object> sorted() {
        throw new UnsupportedOperationException("Unimplemented method 'sorted'");
    }

    @Override
    public Stream<Object> sorted(Comparator<? super Object> comparator) {
        throw new UnsupportedOperationException("Unimplemented method 'sorted'");
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("Unimplemented method 'toArray'");
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        throw new UnsupportedOperationException("Unimplemented method 'toArray'");
    }

}