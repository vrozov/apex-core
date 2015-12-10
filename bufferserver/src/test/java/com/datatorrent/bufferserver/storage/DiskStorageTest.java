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
package com.datatorrent.bufferserver.storage;

import java.net.InetSocketAddress;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.EndWindowTuple;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.bufferserver.support.Controller;
import com.datatorrent.bufferserver.support.Publisher;
import com.datatorrent.bufferserver.support.Subscriber;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import static java.lang.Thread.sleep;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

/**
 *
 */
public class DiskStorageTest
{
  static EventLoopGroup eventloopServer;
  static EventLoopGroup eventloopClient;
  static Server instance;
  static Publisher bsp;
  static Subscriber bss;
  static Controller bsc;
  static int spinCount = 500;
  static Channel channel;
  static ChannelFuture bspChannelFuture;
  static ChannelFuture bssChannelFuture;
  static ChannelFuture bscChannelFuture;

  @BeforeClass
  public static void setupServerAndClients() throws Exception
  {
    eventloopServer = new NioEventLoopGroup(1);

    eventloopClient = new NioEventLoopGroup(2);

    instance = new Server(0, 1024,8);
    instance.setSpoolStorage(new DiskStorage());

    channel = instance.run(eventloopServer);
    InetSocketAddress address = (InetSocketAddress)channel.localAddress();
    assertFalse(address.isUnresolved());

    bsp = new Publisher("MyPublisher");
    bspChannelFuture = bsp.connect(eventloopClient, address);

    bss = new Subscriber("MySubscriber");
    bssChannelFuture = bss.connect(eventloopClient, address);

    bsc = new Controller("MyPublisher");
    bscChannelFuture = bsc.connect(eventloopClient, address);
  }

  @AfterClass
  public static void teardownServerAndClients()
  {
    try {
      channel.disconnect().sync();
    } catch (InterruptedException e) {
      fail();
    } finally {
      eventloopClient.shutdownGracefully();
      eventloopServer.shutdownGracefully();
    }
  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testStorage() throws InterruptedException
  {
    bss.activate(bssChannelFuture, null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);
    bss.flush();

    bsp.activate(bspChannelFuture, null, 0x7afebabe, 0);

    long windowId = 0x7afebabe00000000L;
    bsp.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < 1000; i++) {
      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);
    }

    bsp.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

    windowId++;

    bsp.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < 1000; i++) {
      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);
    }

    bsp.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

    bsp.flush();

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (bss.tupleCount.get() > 2003) {
        break;
      }
    }
    Thread.sleep(10); // wait some more to receive more tuples if possible

    bsp.disconnect().sync();

    bss.disconnect().sync();

    assertEquals(bss.tupleCount.get(), 2004);

    bss = new Subscriber("MySubscriber");
    bss.activate(bss.connect(eventloopClient, channel.localAddress()), null,
        "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);
    bss.flush();

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (bss.tupleCount.get() > 2003) {
        break;
      }
    }
    Thread.sleep(10); // wait some more to receive more tuples if possible
    bss.disconnect().sync();

    assertEquals(bss.tupleCount.get(), 2004);

  }

}
