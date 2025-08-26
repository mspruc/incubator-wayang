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

import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test suite for {@link FlinkCoGroupOperator}.
 */
class FlinkCoGroupOperatorTest extends FlinkOperatorTestBase {

    @Test
    void testExecution() throws Exception {
        // Prepare test data.
        final DataSetChannel.Instance input0 = this.createDataSetChannelInstance(Arrays.asList(
                new Tuple<>(1, "b"), new Tuple<>(1, "c"), new Tuple<>(2, "d"),
                new Tuple<>(3, "e")));
        final DataSetChannel.Instance input1 = this.createDataSetChannelInstance(Arrays.asList(
                new Tuple<>("x", 1), new Tuple<>("y", 1), new Tuple<>("z", 2),
                new Tuple<>("w", 4)));
        final DataSetChannel.Instance output = this.createDataSetChannelInstance();

        // Build the operator.
        final FlinkCoGroupOperator<Tuple, Tuple, Integer> coGroup = new FlinkCoGroupOperator<>(
                new ProjectionDescriptor<>(
                        DataUnitType.createBasicUnchecked(Tuple.class),
                        DataUnitType.createBasic(Integer.class),
                        "field0"),
                new ProjectionDescriptor<>(
                        DataUnitType.createBasicUnchecked(Tuple.class),
                        DataUnitType.createBasic(Integer.class),
                        "field1"),
                DataSetType.createDefaultUnchecked(Tuple.class),
                DataSetType.createDefaultUnchecked(Tuple.class));

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[] { input0, input1 };
        final ChannelInstance[] outputs = new ChannelInstance[] { output };

        // Execute.
        this.evaluate(coGroup, inputs, outputs);

        // Verify the outcome.
        final List<Tuple<Iterable<Tuple<Integer, String>>, Iterable<Tuple<String, Integer>>>> result = output
                .<Tuple<Iterable<Tuple<Integer, String>>, Iterable<Tuple<String, Integer>>>>provideDataSet()
                .collect();
        final Collection<Tuple<Collection<Tuple<Integer, String>>, Collection<Tuple<String, Integer>>>> expectedGroups = new ArrayList<>(
                Arrays.asList(
                        new Tuple<Collection<Tuple<Integer, String>>, Collection<Tuple<String, Integer>>>(
                                Arrays.asList(new Tuple<>(1, "b"),
                                        new Tuple<>(1, "c")),
                                Arrays.asList(new Tuple<>("x", 1),
                                        new Tuple<>("y", 1))),
                        new Tuple<Collection<Tuple<Integer, String>>, Collection<Tuple<String, Integer>>>(
                                Collections.singletonList(new Tuple<>(2, "d")),
                                Collections.singletonList(new Tuple<>("z", 2))),
                        new Tuple<Collection<Tuple<Integer, String>>, Collection<Tuple<String, Integer>>>(
                                Collections.singletonList(new Tuple<>(3, "e")),
                                Collections.emptyList()),
                        new Tuple<Collection<Tuple<Integer, String>>, Collection<Tuple<String, Integer>>>(
                                Collections.emptyList(),
                                Collections.singletonList(new Tuple<>("w", 4)))));

        ResultLoop: for (final Tuple<Iterable<Tuple<Integer, String>>, Iterable<Tuple<String, Integer>>> resultCoGroup : result) {
            for (final Iterator<Tuple<Collection<Tuple<Integer, String>>, Collection<Tuple<String, Integer>>>> i = expectedGroups
                    .iterator(); i.hasNext();) {
                final Tuple<Collection<Tuple<Integer, String>>, Collection<Tuple<String, Integer>>> expectedGroup = i
                        .next();
                if (this.compare(expectedGroup, resultCoGroup)) {
                    i.remove();
                    continue ResultLoop;
                }
            }
            fail(String.format("Unexpected group: %s", resultCoGroup));
        }
        assertTrue(
                expectedGroups.isEmpty(),
                String.format("Missing groups: %s", expectedGroups));
    }

    private boolean compare(
            final Tuple<Collection<Tuple<Integer, String>>, Collection<Tuple<String, Integer>>> expected,
            final Tuple<Iterable<Tuple<Integer, String>>, Iterable<Tuple<String, Integer>>> actual) {
        return this.compareGroup(expected.getField0(), actual.getField0())
                && this.compareGroup(expected.getField1(), actual.getField1());
    }

    private <T> boolean compareGroup(final Collection<T> expected, final Iterable<T> actual) {
        if (expected == null)
            return actual == null;
        if (actual == null)
            return false;

        return WayangCollections.asSet(expected).equals(WayangCollections.asSet(actual));
    }
}
