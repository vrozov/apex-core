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
package com.datatorrent.stram.engine;

import com.datatorrent.api.Sink;

public abstract class AbstractReservoir implements SweepableReservoir
{
  protected Sink<Object> sink;
  private String id;
  protected int count;

  public static AbstractReservoir newReservoir(final String id, final int capacity)
  {
    return new SpscSweepableReservoir(id, capacity);
  }

  protected AbstractReservoir(final String id)
  {
    this.id = id;
  }

  @Override
  public Sink<Object> setSink(Sink<Object> sink)
  {
    try {
      return this.sink;
    } finally {
      this.sink = sink;
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    try {
      return count;
    } finally {
      if (reset) {
        count = 0;
      }
    }
  }

  /**
   * @return the id
   */
  public String getId()
  {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(String id)
  {
    this.id = id;
  }

  public abstract boolean add(Object o);

  public abstract void put(Object o) throws InterruptedException;

  @Override
  public String toString()
  {
    return getClass().getName() + '@' + Integer.toHexString(hashCode()) +
      "{sink=" + sink + ", id=" + id + ", count=" + count + '}';
  }


}
