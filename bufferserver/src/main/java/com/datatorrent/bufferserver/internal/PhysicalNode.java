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
package com.datatorrent.bufferserver.internal;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.bufferserver.util.SerializedData;
import com.datatorrent.netlet.Listener;
import com.datatorrent.netlet.WriteOnlyLengthPrependerClient;

/**
 * PhysicalNode represents one physical subscriber.
 *
 * @since 0.3.2
 */
public class PhysicalNode
{
  private final long starttime;
  private final WriteOnlyLengthPrependerClient client;
  private long processedMessageCount;

  /**
   *
   * @param client
   */
  public PhysicalNode(WriteOnlyLengthPrependerClient client)
  {
    this.client = client;
    starttime = System.currentTimeMillis();
    processedMessageCount = 0;
  }

  /**
   *
   * @return long
   */
  public long getstartTime()
  {
    return starttime;
  }

  /**
   *
   * @return long
   */
  public long getUptime()
  {
    return System.currentTimeMillis() - starttime;
  }

  /**
   *
   * @param d
   * @throws InterruptedException
   */
  private SerializedData blocker;

  public boolean send(SerializedData d)
  {
    if (client.send(d.buffer, d.dataOffset, d.length - (d.dataOffset - d.offset))) {
      processedMessageCount++;
      if (processedMessageCount % 10000000 == 0) {
        logger.info("Max position {} queue size {}", ((Server.Subscriber)client).position, ((Server.Subscriber)client).size);
        ((Server.Subscriber)client).position = 0;
        ((Server.Subscriber)client).size = 0;
      }
      return true;
    }
    blocker = d;
    return false;
  }

  public boolean unblock()
  {
    if (blocker == null) {
      return true;
    }

    if (send(blocker)) {
      blocker = null;
      return true;
    }

    return false;
  }

  public boolean isBlocked()
  {
    return blocker != null;
  }

  /**
   *
   * @return long
   */
  public final long getProcessedMessageCount()
  {
    return processedMessageCount;
  }

  /**
   *
   * @param o
   * @return boolean
   */
  @Override
  public boolean equals(Object o)
  {
    return o == this || (o.getClass() == this.getClass() && o.hashCode() == this.hashCode());
  }

  /**
   *
   * @return int
   */
  public final int getId()
  {
    return client.hashCode();
  }

  /**
   *
   * @return int
   */
  @Override
  public final int hashCode()
  {
    return client.hashCode();
  }

  /**
   * @return the channel
   */
  public Listener.ClientListener getClient()
  {
    return client;
  }

  @Override
  public String toString()
  {
    return "PhysicalNode." + client;
  }

  private static final Logger logger = LoggerFactory.getLogger(PhysicalNode.class);
}
