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

package org.apache.wayang.core.optimizer.costs;

import org.apache.wayang.core.optimizer.OptimizationUtils;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.types.DataSetType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for the {@link NestableLoadProfileEstimator}.
 */
class NestableLoadProfileEstimatorTest {

    public static class SomeOperator extends UnaryToUnaryOperator<Object, Object> {

        protected SomeOperator() {
            super(DataSetType.createDefault(Object.class), DataSetType.createDefault(Object.class), false);
        }

        public int getNumIterations() {
            return 2;
        }
    }

    public static class SomeExecutionOperator extends SomeOperator implements ExecutionOperator {

        @Override
        public Platform getPlatform() {
            return null;
        }

        @Override
        public List<ChannelDescriptor> getSupportedInputChannels(int index) {
            return null;
        }

        @Override
        public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
            return null;
        }

    }

    static final class TestProfileEstimator implements LoadProfileEstimator {
            @Override
            public LoadProfile estimate(EstimationContext context) {
                return null;
            }

            @Override
            public void nest(LoadProfileEstimator loadProfileEstimator) {
                /**
                 * empty for tests purposes.
                 */
            }

            @Override
            public Collection<LoadProfileEstimator> getNestedEstimators() {
                return List.of();
            }

            @Override
            public String getConfigurationKey() {
                return "";
            }

    }

    @Test
    void testSerialization() throws Exception {
        final LoadProfileEstimator loadProfileEstimator = new TestProfileEstimator();

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(loadProfileEstimator);
        objectOutputStream.close();

        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        final ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        final LoadProfileEstimator deserializedObject = (LoadProfileEstimator) objectInputStream.readObject();
        objectInputStream.close();

        assertNotNull(deserializedObject);
    }

    @Test
    void testFromJuelSpecification() {
        String specification = "{" +
                "\"type\":\"juel\"," +
                "\"in\":2," +
                "\"out\":1," +
                "\"p\":0.8," +
                "\"cpu\":\"${3*in0 + 2*in1 + 7*out0}\"," +
                "\"ram\":\"${6*in0 + 4*in1 + 14*out0}\"," +
                "\"overhead\":143," +
                "\"ru\":\"${wayang:logGrowth(0.1, 0.1, 10000, in0+in1)}\"" +
                "}";
        final NestableLoadProfileEstimator estimator = LoadProfileEstimators.createFromSpecification(null,
                specification);
        final LoadProfile estimate = estimator.estimate(new SimpleEstimationContext(
                new CardinalityEstimate[] {
                        new CardinalityEstimate(10, 10, 1d),
                        new CardinalityEstimate(100, 100, 1d)
                },
                new CardinalityEstimate[] { new CardinalityEstimate(200, 300, 1d) },
                new HashMap<String, Double>(),
                1));

        assertEquals(3 * 10 + 2 * 100 + 7 * 200, estimate.getCpuUsage().getLowerEstimate(), 0.01);
        assertEquals(3 * 10 + 2 * 100 + 7 * 300, estimate.getCpuUsage().getUpperEstimate(), 0.01);
        assertEquals(
                OptimizationUtils.logisticGrowth(0.1, 0.1, 10000, 100 + 10),
                estimate.getResourceUtilization(),
                0.000000001);
        assertEquals(143, estimate.getOverheadMillis());
    }

    @Test
    void testFromMathExSpecification() {
        String specification = "{" +
                "\"type\":\"mathex\"," +
                "\"in\":2," +
                "\"out\":1," +
                "\"p\":0.8," +
                "\"cpu\":\"3*in0 + 2*in1 + 7*out0\"," +
                "\"ram\":\"6*in0 + 4*in1 + 14*out0\"," +
                "\"overhead\":143," +
                "\"ru\":\"logGrowth(0.1, 0.1, 10000, in0+in1)\"" +
                "}";
        final NestableLoadProfileEstimator estimator = LoadProfileEstimators.createFromSpecification(null,
                specification);
        final LoadProfile estimate = estimator.estimate(new SimpleEstimationContext(
                new CardinalityEstimate[] {
                        new CardinalityEstimate(10, 10, 1d),
                        new CardinalityEstimate(100, 100, 1d)
                },
                new CardinalityEstimate[] { new CardinalityEstimate(200, 300, 1d) },
                new HashMap<String, Double>(),
                1));

        assertEquals(3 * 10 + 2 * 100 + 7 * 200, estimate.getCpuUsage().getLowerEstimate(), 0.01);
        assertEquals(3 * 10 + 2 * 100 + 7 * 300, estimate.getCpuUsage().getUpperEstimate(), 0.01);
        assertEquals(
                OptimizationUtils.logisticGrowth(0.1, 0.1, 10000, 100 + 10),
                estimate.getResourceUtilization(),
                0.000000001);
        assertEquals(143, estimate.getOverheadMillis());
    }

    @Test
    void testFromJuelSpecificationWithImport() {
        String specification = "{" +
                "\"in\":2," +
                "\"out\":1," +
                "\"import\":[\"numIterations\"]," +
                "\"p\":0.8," +
                "\"cpu\":\"${(3*in0 + 2*in1 + 7*out0) * numIterations}\"," +
                "\"ram\":\"${6*in0 + 4*in1 + 14*out0}\"," +
                "\"overhead\":143," +
                "\"ru\":\"${wayang:logGrowth(0.1, 0.1, 10000, in0+in1)}\"" +
                "}";
        final NestableLoadProfileEstimator estimator = LoadProfileEstimators.createFromSpecification(null,
                specification);
        SomeExecutionOperator execOp = new SomeExecutionOperator();
        HashMap<String, Double> properties = new HashMap<String, Double>();
        properties.put("numIterations", 2d);
        final LoadProfile estimate = estimator.estimate(new SimpleEstimationContext(
                new CardinalityEstimate[] {
                        new CardinalityEstimate(10, 10, 1d),
                        new CardinalityEstimate(100, 100, 1d)
                },
                new CardinalityEstimate[] { new CardinalityEstimate(200, 300, 1d) },
                properties,
                1));

        assertEquals((3 * 10 + 2 * 100 + 7 * 200) * execOp.getNumIterations(),
                estimate.getCpuUsage().getLowerEstimate(), 0.01);
        assertEquals((3 * 10 + 2 * 100 + 7 * 300) * execOp.getNumIterations(),
                estimate.getCpuUsage().getUpperEstimate(), 0.01);
        assertEquals(
                OptimizationUtils.logisticGrowth(0.1, 0.1, 10000, 100 + 10),
                estimate.getResourceUtilization(),
                0.000000001);
        assertEquals(143, estimate.getOverheadMillis());
    }

    @Test
    void testMathExFromSpecificationWithImport() {
        String specification = "{" +
                "\"type\":\"mathex\"," +
                "\"in\":2," +
                "\"out\":1," +
                "\"import\":[\"numIterations\"]," +
                "\"p\":0.8," +
                "\"cpu\":\"(3*in0 + 2*in1 + 7*out0) * numIterations\"," +
                "\"ram\":\"6*in0 + 4*in1 + 14*out0\"," +
                "\"overhead\":143," +
                "\"ru\":\"logGrowth(0.1, 0.1, 10000, in0+in1)\"" +
                "}";
        final NestableLoadProfileEstimator estimator = LoadProfileEstimators.createFromSpecification(null,
                specification);
        SomeExecutionOperator execOp = new SomeExecutionOperator();
        HashMap<String, Double> properties = new HashMap<String, Double>();
        properties.put("numIterations", 2d);
        final LoadProfile estimate = estimator.estimate(new SimpleEstimationContext(
                new CardinalityEstimate[] {
                        new CardinalityEstimate(10, 10, 1d),
                        new CardinalityEstimate(100, 100, 1d)
                },
                new CardinalityEstimate[] { new CardinalityEstimate(200, 300, 1d) },
                properties,
                1));

        assertEquals((3 * 10 + 2 * 100 + 7 * 200) * execOp.getNumIterations(),
                estimate.getCpuUsage().getLowerEstimate(), 0.01);
        assertEquals((3 * 10 + 2 * 100 + 7 * 300) * execOp.getNumIterations(),
                estimate.getCpuUsage().getUpperEstimate(), 0.01);
        assertEquals(
                OptimizationUtils.logisticGrowth(0.1, 0.1, 10000, 100 + 10),
                estimate.getResourceUtilization(),
                0.000000001);
        assertEquals(143, estimate.getOverheadMillis());
    }
}
