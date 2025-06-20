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

import org.apache.wayang.basic.model.DecisionTreeClassificationModel;
import org.apache.wayang.basic.operators.PredictOperators;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.operators.ml.SparkDecisionTreeClassificationOperator;
import org.apache.wayang.spark.operators.ml.SparkPredictOperator;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparkDecisionTreeClassificationOperatorTest extends SparkOperatorTestBase {

    public static List<double[]> trainingX = Arrays.asList(
            new double[]{1, 1},
            new double[]{2, 2},
            new double[]{-1, -1},
            new double[]{-2, -2},
            new double[]{1, -1},
            new double[]{-2, 2}
    );

    public static List<Integer> trainingY = Arrays.asList(
            0, 0, 1, 1, 2, 2
    );

    public static List<double[]> inferenceData = Arrays.asList(
            new double[]{1, 2},
            new double[]{-1, -2},
            new double[]{1, -2}
            );

    public DecisionTreeClassificationModel getModel() {
        // Prepare test data.
        RddChannel.Instance x = this.createRddChannelInstance(trainingX);
        RddChannel.Instance y = this.createRddChannelInstance(trainingY);
        CollectionChannel.Instance output = this.createCollectionChannelInstance();

        SparkDecisionTreeClassificationOperator decisionTreeClassificationOperator = new SparkDecisionTreeClassificationOperator();

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{x, y};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(decisionTreeClassificationOperator, inputs, outputs);

        // Verify the outcome.
        return output.<DecisionTreeClassificationModel>provideCollection().iterator().next();
    }

    @Test
    void testTraining() {
        final DecisionTreeClassificationModel model = getModel();
        assertEquals(2, model.getDepth());
    }

    @Test
    void testInference() {
        // Prepare test data.
        CollectionChannel.Instance input1 = this.createCollectionChannelInstance(Collections.singletonList(getModel()));
        RddChannel.Instance input2 = this.createRddChannelInstance(inferenceData);
        RddChannel.Instance output = this.createRddChannelInstance();

        SparkPredictOperator<double[], Integer> predictOperator = new SparkPredictOperator<>(PredictOperators.decisionTreeClassification());

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input1, input2};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(predictOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> results = output.<Integer>provideRdd().collect();
        assertEquals(3, results.size());
        assertEquals(0, results.get(0).intValue());
        assertEquals(1, results.get(1).intValue());
        assertEquals(2, results.get(2).intValue());
    }
}
