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

package org.apache.wayang.core.util;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for the {@link LimitedInputStream}.
 */
class LimitedInputStreamTest {

    @Test
    void testLimitation() throws IOException {
        // Generate test data.
        byte[] testData = new byte[(int) Byte.MAX_VALUE + 1];
        for (int b = 0; b <= Byte.MAX_VALUE; b++) {
            testData[b] = (byte) b;
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(testData);

        final int limit = 42;
        LimitedInputStream lis = new LimitedInputStream(bais, limit);

        for (int i = 0; i < limit; i++) {
            assertEquals(i, lis.getNumReadBytes());
            assertEquals(i, lis.read());
        }
        assertEquals(42, lis.getNumReadBytes());
        assertEquals(-1, lis.read());
        assertEquals(42, lis.getNumReadBytes());
    }

}
