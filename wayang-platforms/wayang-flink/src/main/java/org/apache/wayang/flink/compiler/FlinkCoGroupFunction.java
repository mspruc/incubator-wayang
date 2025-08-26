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

package org.apache.wayang.flink.compiler;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;
import org.apache.wayang.basic.data.Tuple2;

/**
 * Wrapper of {@Link CoGroupFunction} of Flink for use in Wayang
 */
public class FlinkCoGroupFunction<I0 extends Serializable, I1 extends Serializable>
        implements CoGroupFunction<I0, I1, Tuple2<ArrayList<I0>, ArrayList<I1>>> {

    @Override
    public void coGroup(Iterable<I0> first, Iterable<I1> second,
            Collector<Tuple2<ArrayList<I0>, ArrayList<I1>>> collector)
            throws Exception {
        final ArrayList<I0> list0 = new ArrayList<>();
        final ArrayList<I1> list1 = new ArrayList<>();
        first.forEach(list0::add);
        second.forEach(list1::add);
        collector.collect(new Tuple2<>(list0, list1));
    }
}
