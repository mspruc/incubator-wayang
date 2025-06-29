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

package org.apache.wayang.java.operators;

import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for {@link JavaDistinctOperator}.
 */
class JavaDistinctOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Arrays.asList(0, 1, 1, 2, 2, 6, 6).stream();

        // Build the distinct operator.
        JavaDistinctOperator<Integer> distinctOperator =
                new JavaDistinctOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Execute.
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(distinctOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        assertEquals(4, result.size());
        assertEquals(Arrays.asList(0, 1, 2, 6), result);

    }

}
