/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.bufferserver.packet;

/**
 * <p>EndWindowTuple class.</p>
 *
 * @since 0.3.2
 */
public class EndWindowTuple extends WindowIdTuple
{
  private static final byte[][] serializedTuples = new byte[16000][];

  static {
    for (int i = serializedTuples.length; i-- > 0;) {
      serializedTuples[i] = WindowIdTuple.getSerializedTuple(i);
      serializedTuples[i][0] = MessageType.END_WINDOW_VALUE;
    }
  }

  public EndWindowTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  public static byte[] getSerializedTuple(int windowId)
  {
    return serializedTuples[windowId % serializedTuples.length];
  }

}
