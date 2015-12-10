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
package com.datatorrent.bufferserver.server;

import java.net.InetSocketAddress;
import java.security.SecureRandom;

import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.EndWindowTuple;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.packet.ResetWindowTuple;
import com.datatorrent.bufferserver.support.Controller;
import com.datatorrent.bufferserver.support.Publisher;
import com.datatorrent.bufferserver.support.Subscriber;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;

import static java.lang.Thread.sleep;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class ServerTest
{
  static Server instance;
  static Channel channel;
  static Publisher bsp;
  static Subscriber bss;
  static Controller bsc;
  static int spinCount = 300;
  static NioEventLoopGroup eventloopServer;
  static NioEventLoopGroup eventloopClient;

  static byte[] authToken;

  @BeforeClass
  public static void setupServerAndClients() throws Exception
  {
    eventloopServer = new NioEventLoopGroup(1);
    eventloopClient = new NioEventLoopGroup(1);

    instance = new Server(0, 4096,8);
    channel = instance.run(eventloopServer);
    assertFalse(((InetSocketAddress)channel.localAddress()).isUnresolved());

    SecureRandom random = new SecureRandom();
    authToken = new byte[20];
    random.nextBytes(authToken);
  }

  @AfterClass
  public static void teardownServerAndClients()
  {
    try {
      channel.disconnect().sync();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testNoPublishNoSubscribe() throws InterruptedException
  {
    final InetSocketAddress address = ((InetSocketAddress)channel.localAddress());

    bsp = new Publisher("MyPublisher");
    ChannelFuture bspChannelFuture = bsp.connect(eventloopClient, address);

    bss = new Subscriber("MySubscriber");
    ChannelFuture bssChannelFuture = bss.connect(eventloopClient, address);

    bsp.activate(bspChannelFuture, null, 0L);
    bss.activate(bssChannelFuture, null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);

    bsp.flush();
    bss.flush();

    sleep(100);

    bss.disconnect().sync();
    bsp.disconnect().sync();

    assertEquals(bss.tupleCount.get(), 0);
  }

  @Test(dependsOnMethods = {"testNoPublishNoSubscribe"}, timeOut = 100000)
  @SuppressWarnings("SleepWhileInLoop")
  public void test1Window() throws InterruptedException
  {
    final InetSocketAddress address = ((InetSocketAddress)channel.localAddress());

    bsp = new Publisher("MyPublisher");
    ChannelFuture bspChannelFuture = bsp.connect(eventloopClient, address);

    bss = new Subscriber("MyPublisher");
    ChannelFuture bssChannelFuture = bss.connect(eventloopClient, address);

    bsp.activate(bspChannelFuture, null, 0L);
    bss.activate(bssChannelFuture, null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);

    long resetInfo = 0x7afebabe000000faL;
    bsp.publishMessage(ResetWindowTuple.getSerializedTuple((int)(resetInfo >> 32), 500));

    bsp.flush();
    bss.flush();

    sleep(100);

    bsp.disconnect().sync();
    bss.disconnect().sync();

    assertFalse(bss.resetPayloads.isEmpty());
    assertEquals(1, bss.tupleCount.get());
  }

  @Test(dependsOnMethods = {"test1Window"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testLateSubscriber() throws InterruptedException
  {
    final InetSocketAddress address = ((InetSocketAddress)channel.localAddress());
    
    bss = new Subscriber("MyPublisher");
    bss.activate(bss.connect(eventloopClient, address), null, "BufferServerOutput/BufferServerSubscriber",
        "MyPublisher", 0, null, 0L, 0);
    bss.flush();

    for (int i = 0; i < spinCount; i++) {
      sleep(100);
      if (!bss.resetPayloads.isEmpty()) {
        break;
      }
    }
    sleep(100);

    bss.disconnect();

    assertEquals(bss.tupleCount.get(), 1);
    assertFalse(bss.resetPayloads.isEmpty());
  }

  @Test(dependsOnMethods = {"testLateSubscriber"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testATonOfData() throws InterruptedException
  {
    final InetSocketAddress address = ((InetSocketAddress)channel.localAddress());
    
    bss = new Subscriber("MyPublisher");
    bss.activate(bss.connect(eventloopClient, address), null, "BufferServerOutput/BufferServerSubscriber",
        "MyPublisher", 0, null, 0L, 0);

    bsp = new Publisher("MyPublisher");
    bsp.activate(bsp.connect(eventloopClient, address), null, 0x7afebabe, 0);

    bss.flush();
    bsp.flush();

    long windowId = 0x7afebabe00000000L;

    bsp.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < 100; i++) {
      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);
    }

    bsp.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

    windowId++;

    bsp.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < 100; i++) {
      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);
    }

    bsp.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));
    bsp.flush();

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (bss.tupleCount.get() == 204 + bss.resetPayloads.size()) {
        break;
      }
    }
    sleep(10); // wait some more to receive more tuples if possible

    bsp.disconnect().sync();
    bss.disconnect().sync();

    assertEquals(bss.tupleCount.get(), 204 + bss.resetPayloads.size());
  }

  @Test(dependsOnMethods = {"testATonOfData"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testPurgeNonExistent() throws InterruptedException
  {
    final InetSocketAddress address = ((InetSocketAddress)channel.localAddress());

    bsc = new Controller("MyController");
    bsc.activate(bsc.connect(eventloopClient, address));

    bsc.purge(null, "MyPublisher", 0);
    bsc.flush();

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (bsc.data != null) {
        break;
      }
    }

    bsc.disconnect().sync();

    assertNotNull(bsc.data);

    bss = new Subscriber("MyPublisher");
    bss.activate(bss.connect(eventloopClient, address), null, "BufferServerOutput/BufferServerSubscriber",
        "MyPublisher", 0, null, 0L, 0);
    bss.flush();

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (bss.tupleCount.get() == 205) {
        break;
      }
    }
    sleep(10);
    bss.disconnect().sync();
    assertEquals(bss.tupleCount.get(), 205);
  }

  @Test(dependsOnMethods = {"testPurgeNonExistent"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testPurgeSome() throws InterruptedException
  {
    final InetSocketAddress address = ((InetSocketAddress)channel.localAddress());

    bsc = new Controller("MyController");
    bsc.activate(bsc.connect(eventloopClient, address));

    bsc.purge(null, "MyPublisher", 0x7afebabe00000000L);
    bsc.flush();

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (bsc.data != null) {
        break;
      }
    }
    bsc.disconnect().sync();

    assertNotNull(bsc.data);

    bss = new Subscriber("MyPublisher");
    bss.activate(bss.connect(eventloopClient, address), null, "BufferServerOutput/BufferServerSubscriber",
        "MyPublisher", 0, null, 0L, 0);
    bss.flush();

    for (int i = 0; i < spinCount; i++) {
      sleep(100);
      if (bss.tupleCount.get() == 103) {
        break;
      }
    }
    bss.disconnect().sync();
    assertEquals(bss.tupleCount.get(), 103);
  }

  @Test(dependsOnMethods = {"testPurgeSome"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testPurgeAll() throws InterruptedException
  {
    final InetSocketAddress address = ((InetSocketAddress)channel.localAddress());

    bsc = new Controller("MyController");
    bsc.activate(bsc.connect(eventloopClient, address));

    bsc.purge(null, "MyPublisher", 0x7afebabe00000001L);
    bsc.flush();

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (bsc.data != null) {
        break;
      }
    }
    bsc.disconnect().sync();

    assertNotNull(bsc.data);

    bss = new Subscriber("MyPublisher");
    bss.activate(bss.connect(eventloopClient, address), null, "BufferServerOutput/BufferServerSubscriber",
        "MyPublisher", 0, null, 0L, 0);
    bss.flush();

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (!bss.resetPayloads.isEmpty()) {
        break;
      }
    }
    sleep(10);
    bss.disconnect().sync();
    assertEquals(bss.tupleCount.get(), 1);
  }

  @Test(dependsOnMethods = {"testPurgeAll"})
  public void testRepublish() throws InterruptedException
  {
    testATonOfData();
  }

  @Test(dependsOnMethods = {"testRepublish"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testRepublishLowerWindow() throws InterruptedException
  {
    final InetSocketAddress address = ((InetSocketAddress)channel.localAddress());

    bsp = new Publisher("MyPublisher");
    bsp.activate(bsp.connect(eventloopClient, address), null, 10, 0);

    long windowId = 0L;

    bsp.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < 2; i++) {
      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);
    }

    bsp.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

    windowId++;

    bsp.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < 2; i++) {
      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);
    }

    bsp.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));
    bsp.flush();
    bsp.disconnect().sync();

    bss = new Subscriber("MyPublisher");
    bss.activate(bss.connect(eventloopClient, address), null, "BufferServerOutput/BufferServerSubscriber",
        "MyPublisher", 0, null, 0L, 0);
    bss.flush();
    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (bss.tupleCount.get() == 8) {
        break;
      }
    }
    sleep(10); // wait some more to receive more tuples if possible

    bss.disconnect().sync();

    assertEquals(bss.tupleCount.get(), 8);
  }

  @Test(dependsOnMethods = {"testRepublishLowerWindow"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testReset() throws InterruptedException
  {
    final InetSocketAddress address = ((InetSocketAddress)channel.localAddress());

    bsc = new Controller("MyController");
    bsc.activate(bsc.connect(eventloopClient, address));

    bsc.reset(null, "MyPublisher", 0x7afebabe00000001L);
    bsc.flush();

    for (int i = 0; i < spinCount * 2; i++) {
      sleep(10);
      if (bsc.data != null) {
        break;
      }
    }
    bsc.disconnect().sync();

    assertNotNull(bsc.data);

    bss = new Subscriber("MySubscriber");
    bss.activate(bss.connect(eventloopClient, address), null, "BufferServerOutput/BufferServerSubscriber",
        "MyPublisher", 0, null, 0L, 0);
    bss.flush();

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (bss.tupleCount.get() > 0) {
        break;
      }
    }

    bss.disconnect().sync();

    assertEquals(bss.tupleCount.get(), 0);
  }

  @Test(dependsOnMethods = {"testReset"})
  public void test1WindowAgain() throws InterruptedException
  {
    test1Window();
  }

  @Test(dependsOnMethods = {"test1WindowAgain"})
  public void testResetAgain() throws InterruptedException
  {
    testReset();
  }

  @Test(dependsOnMethods = {"testResetAgain"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testEarlySubscriberForLaterWindow() throws InterruptedException
  {
    final InetSocketAddress address = ((InetSocketAddress)channel.localAddress());

    bss = new Subscriber("MyPublisher");
    bss.activate(bss.connect(eventloopClient, address), null, "BufferServerOutput/BufferServerSubscriber",
      "MyPublisher", 0, null, 49L, 0);
    bss.flush();

    /* wait in a hope that the subscriber is able to reach the server */
    sleep(100);
    bsp = new Publisher("MyPublisher");
    bsp.activate(bsp.connect(eventloopClient, address), null, 0, 0);

    for (int i = 0; i < 100; i++) {
      bsp.publishMessage(BeginWindowTuple.getSerializedTuple(i));

      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);

      bsp.publishMessage(EndWindowTuple.getSerializedTuple(i));
    }

    bsp.flush();

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (bss.tupleCount.get() == 150) {
        break;
      }
    }

    sleep(10);

    bsp.disconnect().sync();
    bss.disconnect().sync();

    assertEquals(bss.tupleCount.get(), 150);

  }

  @Ignore
  @Test(enabled = false, dependsOnMethods = {"testEarlySubscriberForLaterWindow"})
  public void testAuth() throws InterruptedException
  {
    final InetSocketAddress address = ((InetSocketAddress)channel.localAddress());

    instance.setAuthToken(authToken);

    bsp = new Publisher("MyPublisher");
    bsp.setToken(authToken);
    ChannelFuture bspChannelFuture = bsp.connect(eventloopClient, address);

    bss = new Subscriber("MySubscriber");
    bss.setToken(authToken);
    ChannelFuture bssChannelFuture = bss.connect(eventloopClient, address);

    bsp.activate(bspChannelFuture, null, 0L);
    bss.activate(bssChannelFuture, null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);

    long resetInfo = 0x7afebabe000000faL;

    bsp.publishMessage(ResetWindowTuple.getSerializedTuple((int)(resetInfo >> 32), 500));

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (!bss.resetPayloads.isEmpty()) {
        break;
      }
    }
    sleep(10);

    bss.disconnect();
    bsp.disconnect();

    assertEquals(bss.tupleCount.get(), 1);
    assertFalse(bss.resetPayloads.isEmpty());
  }

  @Test(enabled = false, dependsOnMethods = {"testAuth"})
  public void testAuthFailure() throws InterruptedException
  {
    final InetSocketAddress address = ((InetSocketAddress)channel.localAddress());

    byte[] authToken = ServerTest.authToken.clone();
    authToken[0] = (byte)(authToken[0] + 1);

    bsp = new Publisher("MyPublisher");
    bsp.setToken(authToken);
    ChannelFuture bspChannelFuture = bsp.connect(eventloopClient, address);

    bss = new Subscriber("MySubscriber");
    bss.setToken(authToken);
    ChannelFuture bssChannelFuture = bss.connect(eventloopClient, address);

    bsp.activate(bspChannelFuture, null, 0L);
    bss.activate(bssChannelFuture, null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);

    long resetInfo = 0x7afebabe000000faL;

    bsp.publishMessage(ResetWindowTuple.getSerializedTuple((int)(resetInfo >> 32), 500));

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (!bss.resetPayloads.isEmpty()) {
        break;
      }
    }
    sleep(10);

    bss.disconnect();
    bsp.disconnect();

    assertEquals(bss.tupleCount.get(), 0);
    assertTrue(bss.resetPayloads.isEmpty());
  }

  private static final Logger logger = LoggerFactory.getLogger(ServerTest.class);
}
