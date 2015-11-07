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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Queue;

import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.Listener;
import com.datatorrent.netlet.NetletThrowable;
import com.datatorrent.netlet.util.Slice;

public abstract class AbstractClient implements Listener.ClientListener
{
  private static final int THROWABLES_COLLECTION_SIZE = 4;
  public static final int MAX_SENDBUFFER_SIZE;

  protected final SpscArrayQueue<NetletThrowable> throwables;
  protected final SpscArrayQueue<SpscArrayQueue<Slice>> bufferOfBuffers;
  protected final SpscArrayQueue<Slice> freeBuffer;
  protected SpscArrayQueue<Slice> sendBuffer4Offers;
  protected SpscArrayQueue<Slice> sendBuffer4Polls;
  protected final ByteBuffer writeBuffer;
  protected boolean write = true;
  protected SelectionKey key;

  public boolean isConnected()
  {
    return key.isValid() && ((SocketChannel)key.channel()).isConnected();
  }

  public AbstractClient(int writeBufferSize, int sendBufferSize)
  {
    this(ByteBuffer.allocateDirect(writeBufferSize), sendBufferSize);
  }

  public AbstractClient(int sendBufferSize)
  {
    this(8 * 1 * 1024, sendBufferSize);
  }

  public AbstractClient()
  {
    this(8 * 1 * 1024, 1024);
  }

  public AbstractClient(ByteBuffer writeBuffer, int sendBufferSize)
  {
    int i = 1;
    int n = 1;
    do {
      n *= 2;
      i++;
    } while (n != MAX_SENDBUFFER_SIZE);

    bufferOfBuffers = new SpscArrayQueue<SpscArrayQueue<Slice>>(i);

    this.throwables = new SpscArrayQueue<NetletThrowable>(THROWABLES_COLLECTION_SIZE);
    this.writeBuffer = writeBuffer;
    if (sendBufferSize == 0) {
      sendBufferSize = 1024;
    } else if (sendBufferSize % 1024 > 0) {
      sendBufferSize += 1024 - (sendBufferSize % 1024);
    }
    sendBuffer4Polls = sendBuffer4Offers = new SpscArrayQueue<Slice>(sendBufferSize);
    freeBuffer = new SpscArrayQueue<Slice>(sendBufferSize);
  }

  @Override
  public void registered(SelectionKey key)
  {
    this.key = key;
  }

  @Override
  public void connected()
  {
    synchronized (bufferOfBuffers) {
      if (sendBuffer4Polls != sendBuffer4Polls || sendBuffer4Polls.peek() != null) {
        key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
      } else {
        write = false;
        key.interestOps(SelectionKey.OP_READ);
      }
    }
  }

  @Override
  public void disconnected()
  {
    write = true;
  }

  @Override
  public final void read() throws IOException
  {
    SocketChannel channel = (SocketChannel)key.channel();
    int read;
    if ((read = channel.read(buffer())) > 0) {
      this.read(read);
    } else if (read == -1) {
      try {
        channel.close();
      } finally {
        disconnected();
        unregistered(key);
        key.attach(Listener.NOOP_CLIENT_LISTENER);
      }
    } else {
      logger.debug("{} read 0 bytes", this);
    }
  }

  /**
   * @since 1.2.0
   */
  public boolean isReadSuspended()
  {
    return (key.interestOps() & SelectionKey.OP_READ) == 0;
  }

  /**
   * @since 1.2.0
   */
  public boolean suspendReadIfResumed()
  {
    final int interestOps = key.interestOps();
    if ((interestOps & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
      logger.debug("Suspending read on key {} with attachment {}", key, key.attachment());
      key.interestOps(interestOps & ~SelectionKey.OP_READ);
      return true;
    } else {
      return false;
    }
  }

  /**
   * @since 1.2.0
   */
  public boolean resumeReadIfSuspended()
  {
    final int interestOps = key.interestOps();
    if ((interestOps & SelectionKey.OP_READ) == 0) {
      logger.debug("Resuming read on key {} with attachment {}", key, key.attachment());
      key.interestOps(interestOps | SelectionKey.OP_READ);
      key.selector().wakeup();
      return true;
    } else {
      return false;
    }
  }

  /**
   * @deprecated As of release 1.2.0, replaced by {@link #suspendReadIfResumed()}
   */
  @Deprecated
  public void suspendRead()
  {
    key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
  }

  /**
   * @deprecated As of release 1.2.0, replaced by {@link #resumeReadIfSuspended()}
   */
  @Deprecated
  public void resumeRead()
  {
    key.interestOps(key.interestOps() | SelectionKey.OP_READ);
    key.selector().wakeup();
  }

  private int fill()
  {
    int remaining = writeBuffer.remaining();
    remaining:
    while (remaining > 0) {
      Slice f = sendBuffer4Polls.peek();
      if (f == null) {
        synchronized (bufferOfBuffers) {
          f = sendBuffer4Polls.peek();
          if (f == null) {
            if (sendBuffer4Offers == sendBuffer4Polls) {
              break;
            } else {
              sendBuffer4Polls = bufferOfBuffers.poll();
              if (sendBuffer4Polls == null) {
                sendBuffer4Polls = sendBuffer4Offers;
              }
              break remaining;
            }
          }
        }
      }
      if (remaining < f.length) {
        writeBuffer.put(f.buffer, f.offset, remaining);
        f.offset += remaining;
        f.length -= remaining;
        break;
      } else {
        writeBuffer.put(f.buffer, f.offset, f.length);
        remaining -= f.length;
        freeBuffer.offer(sendBuffer4Polls.poll());
      }
    }
    /*
     * switch to the read mode!
     */
    return writeBuffer.flip().remaining();
  }

  @Override
  public final void write() throws IOException
  {
    final SocketChannel channel = (SocketChannel)key.channel();

    /*
     * at first when we enter this function, our buffer is in fill mode.
     */
    int remaining = fill();
    while (remaining > 0) {
      remaining -= channel.write(writeBuffer);
      if (remaining > 0) {
        /*
         * switch back to the fill mode.
         */
        writeBuffer.compact();
        return;
      } else {
        /*
         * switch back to the write mode.
         */
        writeBuffer.clear();

        remaining = fill();
      }
    }

    //assert write;
    int interestOps = key.interestOps();
    //assert (interestOps & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE;
    key.interestOps(interestOps & ~SelectionKey.OP_WRITE);
    write = false;
    /*
     * switch back to fill mode.
     */
    writeBuffer.clear();
  }

  public boolean send(byte[] array)
  {
    return send(array, 0, array.length);
  }

  public boolean send(byte[] array, int offset, int len)
  {
    Slice f = freeBuffer.poll();
    if (f == null) {
      f = new Slice(array, offset, len);
    } else {
      f.buffer = array;
      f.offset = offset;
      f.length = len;
    }

    if (sendBuffer4Offers.offer(f)) {
      /* TODO: Check if we need bufferOfBuffers */
      synchronized (bufferOfBuffers) {
        if (!write) {
          key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
          write = true;
          key.selector().wakeup();
        }
      }

      return true;
    }

    final NetletThrowable throwable = throwables.poll();
    if (throwable != null) {
      NetletThrowable.Util.throwRuntime(throwable);
    }

    if (sendBuffer4Offers.capacity() < MAX_SENDBUFFER_SIZE) {
      synchronized (bufferOfBuffers) {
        if (sendBuffer4Offers != sendBuffer4Polls) {
          bufferOfBuffers.add(sendBuffer4Offers);
        }

        sendBuffer4Offers = new SpscArrayQueue<Slice>(sendBuffer4Offers.capacity() << 1);
        sendBuffer4Offers.add(f);
        if (!write) {
          key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
          write = true;
          key.selector().wakeup();
        }

        return true;
      }
    }

    return false;
  }

  @Override
  public void handleException(Exception cce, EventLoop el)
  {
    logger.debug("Collecting exception in {}", throwables.size(), cce);
    throwables.offer(NetletThrowable.Util.rewrap(cce, el));
  }

  public abstract ByteBuffer buffer();

  public abstract void read(int len);

  @Override
  public void unregistered(SelectionKey key)
  {
    synchronized (bufferOfBuffers) {
      final Queue<Slice> SEND_BUFFER = sendBuffer4Offers;
      sendBuffer4Offers = new SpscArrayQueue<Slice>(0)
      {
        @Override
        public boolean offer(Slice e)
        {
          throw new NetletThrowable.NetletRuntimeException(new UnsupportedOperationException("Client does not own the socket any longer!"), null);
        }

        @Override
        public Slice poll()
        {
          return SEND_BUFFER.poll();
        }

        @Override
        public Slice peek()
        {
          return SEND_BUFFER.peek();
        }
      };
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractClient.class);

  /* implemented here since it requires access to logger. */
  static {
    int size = 32 * 1024;
    final String key = "NETLET.MAX_SENDBUFFER_SIZE";
    String property = System.getProperty(key);
    if (property != null) {
      try {
        size = Integer.parseInt(property);
        if (size <= 0) {
          throw new IllegalArgumentException(key + " needs to be a positive integer which is also power of 2.");
        }

        if ((size & (size - 1)) != 0) {
          size--;
          size |= size >> 1;
          size |= size >> 2;
          size |= size >> 4;
          size |= size >> 8;
          size |= size >> 16;
          size++;
          logger.warn("{} set to {} since {} is not power of 2.", key, size, property);
        }
      } catch (Exception exception) {
        logger.warn("{} set to {} since {} could not be parsed as an integer.", key, size, property, exception);
      }
    }
    MAX_SENDBUFFER_SIZE = size;
  }

}
