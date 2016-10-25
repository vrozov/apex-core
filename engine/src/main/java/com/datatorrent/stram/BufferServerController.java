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
package com.datatorrent.stram;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.client.Controller;
import com.datatorrent.stram.engine.StreamingContainer;

/**
 * Encapsulates buffer server control interface, used by the master for purging data.
 */
class BufferServerController extends Controller
{
  /**
   * Use a single thread group for all buffer server interactions.
   */
  InetSocketAddress addr;

  BufferServerController(String id)
  {
    super(id);
  }

  @Override
  public void onMessage(String message)
  {
    logger.debug("Controller received {}, now disconnecting.", message);
    this.disconnect();
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerController.class);
}
