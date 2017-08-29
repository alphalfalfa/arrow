/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

public class TestFixedSizeBinaryVector {

    private static final int numValues = 10;
    private static final int typeWidth = 7;
    private static byte[][] byteValues;
    static {
        byteValues = new byte[numValues][typeWidth];
        for (int i=0; i<numValues; i++) {
            for (int j=0; j<typeWidth; j++) {
                byteValues[i][j] = ((byte) i);
            }
        }
    }

    @Test
    public void test() {
        BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        NullableFixedSizeBinaryVector fixedSizeBinaryVector = TestUtils.newVector(NullableFixedSizeBinaryVector.class,
                "fixedSizeBinary", new ArrowType.FixedSizeBinary(typeWidth), allocator);
        fixedSizeBinaryVector.allocateNew();
        for (int i=0; i<numValues; i++) {
            fixedSizeBinaryVector.getMutator().set(i, byteValues[i]);
        }
        fixedSizeBinaryVector.getMutator().setValueCount(numValues);

        for (int i=0; i<numValues; i++) {
            byte[] value = fixedSizeBinaryVector.getAccessor().getObject(i);
            assertArrayEquals(byteValues[i], value);
        }
    }

    @Test
    public void testInputWithWrongSize() {
        BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        ArrowType.FixedSizeBinary type = new ArrowType.FixedSizeBinary(typeWidth);
        NullableFixedSizeBinaryVector fixedSizeBinaryVector = TestUtils.newVector(NullableFixedSizeBinaryVector.class,
                "fixedSizeBinary", type, allocator);
        fixedSizeBinaryVector.allocateNew();
        byte[] smallBytes = new byte[typeWidth - 2];
        for (int i=0; i<smallBytes.length; i++) {
            smallBytes[i] = ((byte) i);
        }
        byte[] largeBytes = new byte[typeWidth + 2];
        for (int i=0; i<largeBytes.length; i++) {
            largeBytes[i] = ((byte) i);
        }

        try {
            fixedSizeBinaryVector.getMutator().set(0, smallBytes);
            fail("Only " + type.getByteWidth() + "-byte input data should be allowed");
        } catch (AssertionError ignore) {
        }
        try {
            fixedSizeBinaryVector.getMutator().set(0, largeBytes);
            fail("Only " + type.getByteWidth() + "-byte input data should be allowed");
        } catch (AssertionError ignore) {
        }
    }

}
