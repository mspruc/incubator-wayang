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

package org.apache.wayang.basic.function;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.RecordType;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for the {@link ProjectionDescriptor}.
 */
class ProjectionDescriptorTest {

    @Test
    void testPojoImplementation() {
        final ProjectionDescriptor<Pojo, String> stringDescriptor = new ProjectionDescriptor<>(Pojo.class, String.class, "string");
        final Function<Pojo, String> stringImplementation = stringDescriptor.getJavaImplementation();

        final ProjectionDescriptor<Pojo, Integer> integerDescriptor = new ProjectionDescriptor<>(Pojo.class, Integer.class, "integer");
        final Function<Pojo, Integer> integerImplementation = integerDescriptor.getJavaImplementation();

        assertEquals(
                "testValue",
                stringImplementation.apply(new Pojo("testValue", 1))
        );
        assertNull(stringImplementation.apply(new Pojo(null, 1)));
        assertEquals(
                Integer.valueOf(1),
                integerImplementation.apply(new Pojo("testValue", 1))
        );

    }

    @Test
    void testRecordImplementation() {
        RecordType inputType = new RecordType("a", "b", "c");
        final ProjectionDescriptor<Record, Record> descriptor = ProjectionDescriptor.createForRecords(inputType, "c", "a");
        assertEquals(new RecordType("c", "a"), descriptor.getOutputType());

        final Function<Record, Record> javaImplementation = descriptor.getJavaImplementation();
        assertEquals(
                new Record("world", 10),
                javaImplementation.apply(new Record(10, "hello", "world"))
        );
    }

    public static class Pojo {

        public String string;

        public int integer;

        public Pojo(String string, int integer) {
            this.string = string;
            this.integer = integer;
        }
    }
}
