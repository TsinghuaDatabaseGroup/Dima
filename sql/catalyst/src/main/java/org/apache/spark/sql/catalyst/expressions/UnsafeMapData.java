/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions;

import java.nio.ByteBuffer;

import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.unsafe.Platform;

/**
 * An Unsafe implementation of Map which is backed by raw memory instead of Java objects.
 *
 * Currently we just use 2 UnsafeArrayData to represent UnsafeMapData, with extra 4 bytes at head
 * to indicate the number of bytes of the unsafe key array.
 * [unsafe key array numBytes] [unsafe key array] [unsafe value array]
 */
// TODO: Use a more efficient format which doesn't depend on unsafe array.
public class UnsafeMapData extends MapData {

  private Object baseObject;
  private long baseOffset;

  // The size of this map's backing data, in bytes.
  // The 4-bytes header of key array `numBytes` is also included, so it's actually equal to
  // 4 + key array numBytes + value array numBytes.
  private int sizeInBytes;

  public Object getBaseObject() { return baseObject; }
  public long getBaseOffset() { return baseOffset; }
  public int getSizeInBytes() { return sizeInBytes; }

  private final UnsafeArrayData keys;
  private final UnsafeArrayData values;

  /**
   * Construct a new UnsafeMapData. The resulting UnsafeMapData won't be usable until
   * `pointTo()` has been called, since the value returned by this constructor is equivalent
   * to a null pointer.
   */
  public UnsafeMapData() {
    keys = new UnsafeArrayData();
    values = new UnsafeArrayData();
  }

  /**
   * Update this UnsafeMapData to point to different backing data.
   *
   * @param baseObject the base object
   * @param baseOffset the offset within the base object
   * @param sizeInBytes the size of this map's backing data, in bytes
   */
  public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
    // Read the numBytes of key array from the first 4 bytes.
    final int keyArraySize = Platform.getInt(baseObject, baseOffset);
    final int valueArraySize = sizeInBytes - keyArraySize - 4;
    assert keyArraySize >= 0 : "keyArraySize (" + keyArraySize + ") should >= 0";
    assert valueArraySize >= 0 : "valueArraySize (" + valueArraySize + ") should >= 0";

    keys.pointTo(baseObject, baseOffset + 4, keyArraySize);
    values.pointTo(baseObject, baseOffset + 4 + keyArraySize, valueArraySize);

    assert keys.numElements() == values.numElements();

    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.sizeInBytes = sizeInBytes;
  }

  @Override
  public int numElements() {
    return keys.numElements();
  }

  @Override
  public UnsafeArrayData keyArray() {
    return keys;
  }

  @Override
  public UnsafeArrayData valueArray() {
    return values;
  }

  public void writeToMemory(Object target, long targetOffset) {
    Platform.copyMemory(baseObject, baseOffset, target, targetOffset, sizeInBytes);
  }

  public void writeTo(ByteBuffer buffer) {
    assert(buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
    buffer.position(pos + sizeInBytes);
  }

  @Override
  public UnsafeMapData copy() {
    UnsafeMapData mapCopy = new UnsafeMapData();
    final byte[] mapDataCopy = new byte[sizeInBytes];
    Platform.copyMemory(
      baseObject, baseOffset, mapDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    mapCopy.pointTo(mapDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    return mapCopy;
  }
}
