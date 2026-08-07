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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

public class StreamChannelTest {

    @Test
    void testConstantEncoding() {
        final StreamConstantEncodingChannel channel = new StreamConstantEncodingChannel(
                StreamConstantEncodingChannel.DESCRIPTOR, null);
        final StreamConstantEncodingChannel.Instance instance = channel.createInstance(null, null, 0);

        final List<Integer> testData = IntStream.range(0, 2048).boxed().toList();

        instance.accept(testData.stream());

        final List<?> result = instance.provideStream().toList();

        assertEquals(2048, result.size());
    }

    @Test
    void testConstantEncodingWithDuplicates() {
        final StreamConstantEncodingChannel channel = new StreamConstantEncodingChannel(
                StreamConstantEncodingChannel.DESCRIPTOR, null);
        final StreamConstantEncodingChannel.Instance instance = channel.createInstance(null, null, 0);

        final List<Integer> testData = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            for (int j = 0; j < 1024; j++) {
                testData.add(i);
            }
        }

        instance.accept(testData.stream());
        final List<?> result = instance.provideStream().toList();

        assertEquals(50, result.size());
    }

    @Test
    void testConstantEncodingCollectionWithDuplicates() {
        final StreamConstantEncodingChannel channel = new StreamConstantEncodingChannel(
                StreamConstantEncodingChannel.DESCRIPTOR, null);
        final StreamConstantEncodingChannel.Instance instance = channel.createInstance(null, null, 0);

        final List<Integer> testData = new ArrayList<>();
        for (int i = 0; i < 512; i++) {
            testData.add(i);
            testData.add(i);
        }

        instance.accept(testData);
        final List<?> result = instance.provideStream().toList();

        assertEquals(512, result.size());
    }

    // pbt for output always being shorter than or equal to the input size
    @Test
    void outputIsShorterThanOrEqualToInput() {
        final Random random = new Random(42);

        for (int trial = 0; trial < 100; trial++) {
            final List<Integer> input = generateRandomList(random, 100);

            final StreamConstantEncodingChannel channel = new StreamConstantEncodingChannel(StreamConstantEncodingChannel.DESCRIPTOR, null);
            final StreamConstantEncodingChannel.Instance instance = channel.createInstance(null, null, 0);
            instance.accept(input.stream());
            final List<?> output = instance.provideStream().toList();

            assertTrue(output.size() <= input.size(), String.format("Trial %d: output size %d exceeds input size %d",
                    trial, output.size(), input.size()));
        }
    }

    @Test
    void testConstantEncodingEmpty() {
        final StreamConstantEncodingChannel channel = new StreamConstantEncodingChannel(
                StreamConstantEncodingChannel.DESCRIPTOR, null);
        final StreamConstantEncodingChannel.Instance instance = channel.createInstance(null, null, 0);

        instance.accept(Stream.empty());
        final List<?> result = instance.provideStream().toList();

        assertTrue(result.isEmpty());
    }

    private List<Integer> generateRandomList(final Random random, final int maxSize) {
        final int size = random.nextInt(maxSize);
        final List<Integer> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(random.nextInt(1000));
        }
        return list;
    }
}
