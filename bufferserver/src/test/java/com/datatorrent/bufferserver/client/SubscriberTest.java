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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.EndWindowTuple;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.packet.ResetWindowTuple;
import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.bufferserver.support.Publisher;
import com.datatorrent.bufferserver.support.Subscriber;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.netlet.DefaultEventLoop;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 *
 */
public class SubscriberTest
{
  private static final Logger logger = LoggerFactory.getLogger(SubscriberTest.class);
  static Server instance;
  static EventLoopGroup eventloopServer;
  static EventLoopGroup eventloopClient;
  static Channel channel;

  @BeforeClass
  public static void setupServerAndClients() throws Exception
  {
    eventloopServer = new NioEventLoopGroup(1);
    eventloopClient = new NioEventLoopGroup(1);

    instance = new Server(0, 64, 2);
    channel = instance.run(eventloopServer);
    assertTrue(channel.localAddress() instanceof InetSocketAddress);
    assertFalse(((InetSocketAddress)channel.localAddress()).isUnresolved());
  }

  @AfterClass
  public static void teardownServerAndClients()
  {
    try {
      channel.close().sync();
    } catch (InterruptedException e) {
      fail();
    } finally {
      try {
        eventloopClient.shutdownGracefully().sync();
        eventloopServer.shutdownGracefully().sync();
      } catch (InterruptedException e) {
        fail();
      }
    }
  }

  @Test(timeOut = 100000)
  @SuppressWarnings("SleepWhileInLoop")
  public void test() throws InterruptedException
  {
    InetSocketAddress address = ((InetSocketAddress)channel.localAddress());

    final Publisher bsp1 = new Publisher("MyPublisher");
    ChannelFuture bsp1ChannelFuture = bsp1.connect(eventloopClient, address);

    final Subscriber bss1 = new Subscriber("MySubscriber")
    {
      @Override
      public void beginWindow(int windowId)
      {
        super.beginWindow(windowId);
        if (windowId > 9) {
          synchronized (SubscriberTest.this) {
            SubscriberTest.this.notifyAll();
          }
        }
      }

      @Override
      public String toString()
      {
        return "BufferServerSubscriber";
      }

    };
    ChannelFuture bss1ChanelFuture = bss1.connect(eventloopClient, address);

    final int baseWindow = 0x7afebabe;
    bsp1.activate(bsp1ChannelFuture, null, baseWindow, 0);
    bss1.activate(bss1ChanelFuture, null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);
    bss1.flush();

    final AtomicBoolean publisherRun = new AtomicBoolean(true);
    new Thread("publisher")
    {
      @Override
      @SuppressWarnings("SleepWhileInLoop")
      public void run()
      {
        bsp1.publishMessage(ResetWindowTuple.getSerializedTuple(baseWindow, 500));

        long windowId = 0x7afebabe00000000L;
        try {
          while (publisherRun.get()) {
            bsp1.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

            bsp1.publishMessage(PayloadTuple.getSerializedTuple(0, 0));

            bsp1.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

            bsp1.flush();

            windowId++;
            Thread.sleep(5);
          }
        } catch (InterruptedException | CancelledKeyException e) {
          logger.debug("{}", e);
        } finally {
          logger.debug("publisher the middle of window = {}", Codec.getStringWindowId(windowId));
        }
      }

    }.start();

    synchronized (this) {
      wait();
    }

    publisherRun.set(false);

    bsp1.disconnect().sync();
    bss1.disconnect().sync();

    /*
     * At this point, we know that both the publishers and the subscribers have gotten at least window Id 10.
     * So we go ahead and make the publisher publish from 5 onwards with different data and have subscriber
     * subscribe from 8 onwards. What we should see is that subscriber gets the new data from 8 onwards.
     */
    final Publisher bsp2 = new Publisher("MyPublisher");
    bsp2.activate(bsp2.connect(eventloopClient, address), null, 0x7afebabe, 5);

    final Subscriber bss2 = new Subscriber("MyPublisher")
    {
      @Override
      public void beginWindow(int windowId)
      {
        super.beginWindow(windowId);
        if (windowId > 14) {
          synchronized (SubscriberTest.this) {
            SubscriberTest.this.notifyAll();
          }
        }
      }

    };

    bss2.activate(bss2.connect(eventloopClient, address), null, "BufferServerOutput/BufferServerSubscriber",
        "MyPublisher", 0, null, 0x7afebabe00000008L, 0);
    bss2.flush();


    publisherRun.set(true);
    new Thread("publisher")
    {
      @Override
      @SuppressWarnings("SleepWhileInLoop")
      public void run()
      {
        long windowId = 0x7afebabe00000005L;
        try {
          while (publisherRun.get()) {
            bsp2.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

            byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
            buff[buff.length - 1] = 'a';
            bsp2.publishMessage(buff);

            bsp2.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

            bsp2.flush();

            windowId++;
            Thread.sleep(5);
          }
        } catch (InterruptedException | CancelledKeyException e) {
          logger.debug("", e);
        } finally {
          logger.debug("publisher in the middle of window = {}", Codec.getStringWindowId(windowId));
        }
      }

    }.start();

    synchronized (this) {
      wait();
    }

    publisherRun.set(false);

    bsp2.disconnect().sync();
    bss2.disconnect().sync();

    assertTrue((bss2.lastPayload.getWindowId() - 8) * 3 <= bss2.tupleCount.get());
  }

}
