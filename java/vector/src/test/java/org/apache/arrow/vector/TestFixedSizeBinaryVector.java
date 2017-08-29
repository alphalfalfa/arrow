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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.holders.FixedSizeBinaryHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class TestFixedSizeBinaryVector {

    private static final int numValues = 123;
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

    private BufferAllocator allocator;
    private NullableFixedSizeBinaryVector fixedSizeBinaryVector;

    @Before
    public void setUp() throws Exception {
        allocator = new RootAllocator(Integer.MAX_VALUE);
        fixedSizeBinaryVector = TestUtils.newVector(NullableFixedSizeBinaryVector.class, "fixedSizeBinary",
            new ArrowType.FixedSizeBinary(typeWidth), allocator);
        fixedSizeBinaryVector.allocateNew();
    }

    @Test
    public void test() {
        for (int i=0; i<numValues; i++) {
            fixedSizeBinaryVector.getMutator().set(i, byteValues[i]);
        }
        fixedSizeBinaryVector.getMutator().setValueCount(numValues);

        for (int i=0; i<numValues; i++) {
            byte[] value = fixedSizeBinaryVector.getAccessor().getObject(i);
            assertArrayEquals(byteValues[i], value);
        }
    }

    private static void failWithException(String message) throws Exception {
        throw new Exception(message);
    }

    @Test
    public void testMutator() throws Exception {
        int smallTypeWidth = typeWidth - 2;
        byte[] smallBytes = new byte[smallTypeWidth];
        for (int i = 0; i< smallTypeWidth; i++) {
            smallBytes[i] = ((byte) i);
        }


        int largeTypeWidth = typeWidth + 2;
        byte[] largeBytes = new byte[largeTypeWidth];
        for (int i = 0; i< largeTypeWidth; i++) {
            largeBytes[i] = ((byte) i);
        }

        ArrowBuf smallBuf = allocator.buffer(smallTypeWidth);
        smallBuf.setBytes(0, smallBytes);

        ArrowBuf largeBuf = allocator.buffer(largeTypeWidth);
        smallBuf.setBytes(0, smallBytes);

        FixedSizeBinaryHolder smallHolder = new FixedSizeBinaryHolder();
        smallHolder.byteWidth = smallTypeWidth;
        smallHolder.index = 0;
        smallHolder.buffer = smallBuf;

        FixedSizeBinaryHolder largeHolder = new FixedSizeBinaryHolder();
        largeHolder.byteWidth = largeTypeWidth;
        largeHolder.index = 0;
        largeHolder.buffer = largeBuf;

        String errorMsg = "Only " + typeWidth + "-byte input data should be allowed";
        NullableFixedSizeBinaryVector.Mutator mutator = fixedSizeBinaryVector.getMutator();

        try {
            mutator.set(0, smallBytes);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {}
        try {
            mutator.set(0, largeBytes);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {}

        try {
            mutator.set(0, smallHolder);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {}

        try {
            mutator.set(0, largeHolder);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {}

        try {
            mutator.set(0, 1, smallBuf);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {}

        try {
            mutator.set(0, 1, largeBuf);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {}

        try {
            mutator.setSafe(0, smallBytes);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {}
        try {
            mutator.setSafe(0, largeBytes);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {}

        try {
            mutator.setSafe(0, smallHolder);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {}

        try {
            mutator.setSafe(0, largeHolder);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {}

        try {
            mutator.setSafe(0, 1, smallBuf);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {}

        try {
            mutator.setSafe(0, 1, largeBuf);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {}
    }

}
