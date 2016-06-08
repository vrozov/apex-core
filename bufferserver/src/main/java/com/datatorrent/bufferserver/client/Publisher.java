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
package com.datatorrent.bufferserver.client;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.PublishRequestTuple;
import com.datatorrent.netlet.AbstractWriteOnlyLengthPrependerClient;

/**
 * <p>Abstract Publisher class.</p>
 *
 * @since 0.3.2
 */
public abstract class Publisher extends AbstractWriteOnlyLengthPrependerClient
{
  private byte[] token;
  private final String id;

  public Publisher(String id)
  {
    this(id, 32 * 1024);
  }

  public Publisher(String id, int sendBufferCapacity)
  {
    super(64 * 1024, sendBufferCapacity);
    this.id = id;
  }

  /**
   *
   * @param windowId
   */
  public void activate(String version, long windowId)
  {
    if (token != null) {
      send(token);
    }
    send(PublishRequestTuple.getSerializedRequest(version, id, windowId));
  }

  public void setToken(byte[] token)
  {
    this.token = token;
  }

  @Override
  public String toString()
  {
    return "Publisher{" + "id=" + id + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
}
