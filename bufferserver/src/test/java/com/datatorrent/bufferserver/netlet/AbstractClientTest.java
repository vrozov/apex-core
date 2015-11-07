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
package com.datatorrent.bufferserver.netlet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.jctools.queues.SpscArrayQueue;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.Listener;
import com.datatorrent.netlet.OptimizedEventLoop;
import com.datatorrent.netlet.util.Slice;

import static java.lang.Thread.sleep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AbstractClientTest
{
  private final CountDownLatch read = new CountDownLatch(1);

  private static class SpscArrayQueueWrapper extends SpscArrayQueue<Slice>
  {
    private SpscArrayQueueWrapper(int capacity)
    {
      super(capacity);
    }

    @Override
    public Slice poll()
    {
      final Slice f = super.poll();
      if (f != null) {
        assertTrue("Unexpected slice length: " + f.length, f.length > 0);
      }
      return f;
    }

    @Override
    public Slice peek()
    {
      final Slice f = super.peek();
      if (f != null) {
        assertTrue("Unexpected slice length: " + f.length, f.length > 0);
      }
      return f;
    }

  }

  private static class EchoServer extends AbstractClient implements Listener.ServerListener
  {
    ByteBuffer buffer = ByteBuffer.allocate(1024 * 4);

    @Override
    public ClientListener getClientConnection(SocketChannel client, ServerSocketChannel server)
    {
      return this;
    }

    @Override
    public ByteBuffer buffer()
    {
      buffer.clear();
      return buffer;
    }

    @Override
    public String toString()
    {
      return "EchoClient{" + "buffer=" + buffer + '}';
    }

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void read(int len)
    {
      byte[] array = new byte[len];
      System.arraycopy(buffer.array(), 0, array, 0, len);
      try {
        while (!send(array, 0, len)) {
          sleep(5);
        }
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }

    @Override
    public void connected()
    {
    }

    @Override
    public void disconnected()
    {
    }

  }

  private class EchoClient extends AbstractClient
  {
    public static final int BUFFER_CAPACITY = 8 * 1024 + 1;
    ByteBuffer buffer = ByteBuffer.allocate(BUFFER_CAPACITY);

    @Override
    public String toString()
    {
      return getClass().getName() + '@' + Integer.toHexString(hashCode()) + "{buffer=" + buffer + '}';
    }

    @Override
    public ByteBuffer buffer()
    {
      return buffer;
    }

    @Override
    public void read(int len)
    {
      if (buffer.position() == buffer.capacity()) {
        buffer.flip();
        read.countDown();

        int i = 0;
        LongBuffer lb = buffer.asLongBuffer();
        while (lb.hasRemaining()) {
          assertEquals(i++, lb.get());
        }

        assert (i == BUFFER_CAPACITY / 8);
      }
    }

    private SpscArrayQueue<Slice> getSendBuffer4Polls()
    {
      return sendBuffer4Polls;
    }

    private void setSendBuffer4Polls(SpscArrayQueue<Slice> circularBuffer)
    {
      sendBuffer4Polls = circularBuffer;
    }

    private SpscArrayQueue<Slice> getSendBuffer4Offers()
    {
      return sendBuffer4Offers;
    }

    private void setSendBuffer4Offers(SpscArrayQueue<Slice> circularBuffer)
    {
      sendBuffer4Offers = circularBuffer;
    }

    private ByteBuffer getWriteBuffer()
    {
      return writeBuffer;
    }

  }

  public void sendData()
  {
  }

  @SuppressWarnings("SleepWhileInLoop")
  private void verifySendReceive(final DefaultEventLoop eventLoop, final int port) throws IOException, InterruptedException
  {
    EchoServer server = new EchoServer();
    EchoClient client = new EchoClient();

    new Thread(eventLoop).start();

    eventLoop.start("localhost", port, server);
    eventLoop.connect(new InetSocketAddress("localhost", port), client);

    ByteBuffer outboundBuffer = ByteBuffer.allocate(EchoClient.BUFFER_CAPACITY);
    LongBuffer lb = outboundBuffer.asLongBuffer();

    int i = 0;
    while (lb.hasRemaining()) {
      lb.put(i++);
    }

    boolean odd = false;
    outboundBuffer.position(i * 8);
    while (outboundBuffer.hasRemaining()) {
      outboundBuffer.put((byte)(odd ? 0x55 : 0xaa));
      odd = !odd;
    }

    byte[] array = outboundBuffer.array();
    while (!client.send(array, 0, array.length)) {
      sleep(5);
    }

    read.await(100, TimeUnit.MILLISECONDS);

    eventLoop.disconnect(client);
    eventLoop.stop(server);
    eventLoop.stop();
    assertEquals(0, read.getCount());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testWithDefault() throws IOException, InterruptedException
  {
    verifySendReceive(new DefaultEventLoop("test"), 5033);
  }

  @Test
  public void testWithOptimized() throws IOException, InterruptedException
  {
    verifySendReceive(new OptimizedEventLoop("test"), 5034);
  }

  @Test
  public void testCreateEventLoop() throws IOException
  {
    assertEquals(OptimizedEventLoop.class, DefaultEventLoop.createEventLoop("test").getClass());
    System.setProperty(DefaultEventLoop.eventLoopPropertyName, "");
    assertEquals(DefaultEventLoop.class, DefaultEventLoop.createEventLoop("test").getClass());
    System.setProperty(DefaultEventLoop.eventLoopPropertyName, "false");
    assertEquals(OptimizedEventLoop.class, DefaultEventLoop.createEventLoop("test").getClass());
    System.setProperty(DefaultEventLoop.eventLoopPropertyName, "true");
    assertEquals(DefaultEventLoop.class, DefaultEventLoop.createEventLoop("test").getClass());
    System.setProperty(DefaultEventLoop.eventLoopPropertyName, "no");
    assertEquals(OptimizedEventLoop.class, DefaultEventLoop.createEventLoop("test").getClass());
    System.setProperty(DefaultEventLoop.eventLoopPropertyName, "yes");
    assertEquals(DefaultEventLoop.class, DefaultEventLoop.createEventLoop("test").getClass());
  }

  @Test
  public void testOneSlice() throws IOException
  {
    EchoClient client = new EchoClient();
    client.setSendBuffer4Polls(new SpscArrayQueueWrapper(1));
    client.setSendBuffer4Offers(client.getSendBuffer4Polls());
    client.key = new SelectionKey()
    {
      private int interestOps;

      SocketChannel channel = new SocketChannel(null)
      {
        @Override
        public SocketChannel bind(SocketAddress local) throws IOException
        {
          return null;
        }

        @Override
        public <T> SocketChannel setOption(SocketOption<T> name, T value) throws IOException
        {
          return null;
        }

        @Override
        public SocketChannel shutdownInput() throws IOException
        {
          return null;
        }

        @Override
        public SocketChannel shutdownOutput() throws IOException
        {
          return null;
        }

        @Override
        public Socket socket()
        {
          return null;
        }

        @Override
        public boolean isConnected()
        {
          return false;
        }

        @Override
        public boolean isConnectionPending()
        {
          return false;
        }

        @Override
        public boolean connect(SocketAddress remote) throws IOException
        {
          return false;
        }

        @Override
        public boolean finishConnect() throws IOException
        {
          return false;
        }

        @Override
        public SocketAddress getRemoteAddress() throws IOException
        {
          return null;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException
        {
          return 0;
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException
        {
          return 0;
        }

        @Override
        public int write(ByteBuffer src) throws IOException
        {
          final int remaining = src.remaining();
          src.position(src.position() + remaining);
          return remaining;
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
        {
          return 0;
        }

        @Override
        protected void implCloseSelectableChannel() throws IOException
        {

        }

        @Override
        protected void implConfigureBlocking(boolean block) throws IOException
        {

        }

        @Override
        public SocketAddress getLocalAddress() throws IOException
        {
          return null;
        }

        @Override
        public <T> T getOption(SocketOption<T> name) throws IOException
        {
          return null;
        }

        @Override
        public Set<SocketOption<?>> supportedOptions()
        {
          return null;
        }
      };

      @Override
      public SelectableChannel channel()
      {
        return channel;
      }

      @Override
      public Selector selector()
      {
        return null;
      }

      @Override
      public boolean isValid()
      {
        return true;
      }

      @Override
      public void cancel()
      {

      }

      @Override
      public int interestOps()
      {
        return interestOps;
      }

      @Override
      public SelectionKey interestOps(int ops)
      {
        if ((ops & ~OP_WRITE) != 0) {
          throw new IllegalArgumentException();
        }
        interestOps = ops;
        return this;
      }

      @Override
      public int readyOps()
      {
        return OP_WRITE;
      }
    };
    client.send(new byte[client.getWriteBuffer().remaining()]);
    client.key.interestOps(SelectionKey.OP_WRITE);
    client.write();
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractClientTest.class);
}
