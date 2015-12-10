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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.internal.DataList;
import com.datatorrent.bufferserver.internal.FastDataList;
import com.datatorrent.bufferserver.internal.LogicalNode;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.packet.PublishRequestTuple;
import com.datatorrent.bufferserver.packet.PurgeRequestTuple;
import com.datatorrent.bufferserver.packet.ResetRequestTuple;
import com.datatorrent.bufferserver.packet.SubscribeRequestTuple;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.bufferserver.storage.Storage;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.util.VarInt;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * The buffer server application<p>
 * <br>
 *
 * @since 0.3.2
 */
public class Server
{
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024 * 1024;
  public static final int DEFAULT_NUMBER_OF_CACHED_BLOCKS = 8;
  private final int port;
  private String identity;
  private Storage storage;
  private InetSocketAddress address;
  private final ExecutorService serverHelperExecutor;
  private final ExecutorService storageHelperExecutor;

  private byte[] authToken;

  /**
   * @param port - port number to bind to or 0 to auto select a free port
   */
  public Server(int port)
  {
    this(port, DEFAULT_BUFFER_SIZE, DEFAULT_NUMBER_OF_CACHED_BLOCKS);
  }

  public Server(int port, int blocksize, int numberOfCacheBlocks)
  {
    this.port = port;
    this.blockSize = blocksize;
    this.numberOfCacheBlocks = numberOfCacheBlocks;
    serverHelperExecutor = Executors.newSingleThreadExecutor(new NameableThreadFactory("ServerHelper"));
    final ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(numberOfCacheBlocks);
    final NameableThreadFactory threadFactory = new NameableThreadFactory("StorageHelper");
    storageHelperExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, workQueue, threadFactory,
        new ThreadPoolExecutor.CallerRunsPolicy());
  }

  public void setSpoolStorage(Storage storage)
  {
    this.storage = storage;
  }

  /*
  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception
  {
    address = (InetSocketAddress)ctx.channel().localAddress();
    logger.info("Server started listening at {}", address);
    notifyAll();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception
  {
    serverHelperExecutor.shutdown();
    storageHelperExecutor.shutdown();
    try {
      serverHelperExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      logger.debug("Executor Termination", ex);
    }
    logger.info("Server stopped listening at {}", address);
  }
  */

  public Channel run(EventLoopGroup eventLoop)
  {
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(eventLoop)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.TRACE))
        .childHandler(
          new ChannelInitializer<SocketChannel>()
          {
            @Override
            public void initChannel(SocketChannel ch) throws Exception
            {
              final ChannelPipeline pipeline = ch.pipeline();
              pipeline.addLast("logger", new LoggingHandler(LogLevel.TRACE));
              pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
              pipeline.addLast("bytesDecoder", new ByteArrayDecoder());
              pipeline.addLast("requestHandler", new UnidentifiedClient());
              pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
              pipeline.addLast("bytesEncoder", new ByteArrayEncoder());
            }
          }
        )
        .option(ChannelOption.SO_BACKLOG, 128)
        .childOption(ChannelOption.SO_KEEPALIVE, true);

    try {
      return bootstrap.bind(port).sync().channel();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void setAuthToken(byte[] authToken)
  {
    this.authToken = authToken;
  }

  /**
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception
  {
    int port;
    if (args.length > 0) {
      port = Integer.parseInt(args[0]);
    } else {
      port = 0;
    }
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
    try {
      Channel channel = new Server(port).run(eventLoopGroup);
      channel.closeFuture().sync();
    } finally {
      eventLoopGroup.shutdownGracefully().sync();
    }
  }

  @Override
  public String toString()
  {
    return identity;
  }

  private final ConcurrentHashMap<String, DataList> publisherBuffers = new ConcurrentHashMap<>(1, 0.75f, 1);
  private final ConcurrentHashMap<String, LogicalNode> subscriberGroups = new ConcurrentHashMap<String, LogicalNode>();
  private final ConcurrentHashMap<String, Channel> publisherChannels = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Channel> subscriberChannels = new ConcurrentHashMap<>();
  private final int blockSize;
  private final int numberOfCacheBlocks;

  private void handlePurgeRequest(final PurgeRequestTuple request, final ChannelHandlerContext ctx) throws
      IOException
  {
    DataList dl;
    dl = publisherBuffers.get(request.getIdentifier());

    byte[] message;
    if (dl == null) {
      message = ("Invalid identifier '" + request.getIdentifier() + "'").getBytes();
    } else {
      dl.purge((long)request.getBaseSeconds() << 32 | request.getWindowId());
      message = ("Request sent for processing: " + request).getBytes();
    }

    final byte[] tuple = PayloadTuple.getSerializedTuple(0, message.length);
    System.arraycopy(message, 0, tuple, tuple.length - message.length, message.length);
    ctx.channel().write(tuple);
    ctx.channel().flush();
  }

  public void purge(long windowId)
  {
    for (DataList dataList: publisherBuffers.values()) {
      dataList.purge(windowId);
    }
  }

  private void handleResetRequest(final ResetRequestTuple request, final ChannelHandlerContext ctx) throws IOException
  {
    DataList dl;
    dl = publisherBuffers.remove(request.getIdentifier());

    byte[] message;
    if (dl == null) {
      message = ("Invalid identifier '" + request.getIdentifier() + "'").getBytes();
    } else {
      Channel previous = publisherChannels.remove(request.getIdentifier());
      if (previous != null) {
        previous.disconnect();
      }
      dl.reset();
      message = ("Request sent for processing: " + request).getBytes();
    }

    final byte[] tuple = PayloadTuple.getSerializedTuple(0, message.length);
    System.arraycopy(message, 0, tuple, tuple.length - message.length, message.length);
    ctx.channel().write(tuple);
    ctx.channel().flush();
  }

  /**
   *
   * @param request
   * @param connection
   * @return
   */
  public LogicalNode handleSubscriberRequest(final SubscribeRequestTuple request, final ChannelHandlerContext ctx)
  {
    String identifier = request.getIdentifier();
    String type = request.getStreamType();
    String upstream_identifier = request.getUpstreamIdentifier();

    // Check if there is a logical node of this type, if not create it.
    final LogicalNode ln;
    if (subscriberGroups.containsKey(type)) {
      //logger.debug("adding to exiting group = {}", subscriberGroups.get(type));
      /*
       * close previous connection with the same identifier which is guaranteed to be unique.
       */
      final Channel previous = subscriberChannels.put(identifier, ctx.channel());
      if (previous != null) {
        previous.disconnect();
      }

      ln = subscriberGroups.get(type);
      serverHelperExecutor.submit(new Runnable()
      {
        @Override
        public void run()
        {
          ln.boot();
          ln.addConnection(connection);
          ln.catchUp();
        }
      });
    } else {
      /*
       * if there is already a datalist registered for the type in which this client is interested,
       * then get a iterator on the data items of that data list. If the datalist is not registered,
       * then create one and register it. Hopefully this one would be used by future upstream nodes.
       */
      final DataList dl;
      if (publisherBuffers.containsKey(upstream_identifier)) {
        dl = publisherBuffers.get(upstream_identifier);
        //logger.debug("old list = {}", dl);
      } else {
        dl = Tuple.FAST_VERSION.equals(request.getVersion()) ?
            new FastDataList(upstream_identifier, blockSize, numberOfCacheBlocks) :
            new DataList(upstream_identifier, blockSize, numberOfCacheBlocks);
        publisherBuffers.put(upstream_identifier, dl);
        //logger.debug("new list = {}", dl);
      }

      long skipWindowId = (long)request.getBaseSeconds() << 32 | request.getWindowId();
      ln = new LogicalNode(identifier, upstream_identifier, type, dl.newIterator(skipWindowId), skipWindowId);

      int mask = request.getMask();
      if (mask != 0) {
        for (Integer bs : request.getPartitions()) {
          ln.addPartition(bs, mask);
        }
      }

      subscriberGroups.put(type, ln);
      serverHelperExecutor.submit(new Runnable()
      {
        @Override
        public void run()
        {
          ln.addConnection(ctx.channel());
          ln.catchUp();
          dl.addDataListener(ln);
        }
      });
    }

    return ln;
  }

  /**
   *
   * @param request
   * @param connection
   * @return
   */
  public DataList handlePublisherRequest(final PublishRequestTuple request, final ChannelHandlerContext ctx)
  {
    String identifier = request.getIdentifier();

    DataList dl;

    if (publisherBuffers.containsKey(identifier)) {
      /*
       * close previous connection with the same identifier which is guaranteed to be unique.
       */
      final Channel previous = publisherChannels.put(identifier, ctx.channel());
      if (previous != null) {
        previous.disconnect();
      }

      dl = publisherBuffers.get(identifier);
      try {
        dl.rewind(request.getBaseSeconds(), request.getWindowId());
      } catch (IOException ie) {
        throw new RuntimeException(ie);
      }
    } else {
      dl = Tuple.FAST_VERSION.equals(request.getVersion()) ?
          new FastDataList(identifier, blockSize, numberOfCacheBlocks) :
          new DataList(identifier, blockSize, numberOfCacheBlocks);
      publisherBuffers.put(identifier, dl);
    }
    dl.setSecondaryStorage(storage, storageHelperExecutor);

    return dl;
  }

  /*
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
  {
    ctx.close();
    if (cause instanceof RuntimeException) {
      throw (RuntimeException)cause;
    } else {
      throw new RuntimeException(cause);
    }
  }
  */

  private class UnidentifiedClient extends ChannelHandlerAdapter
  {
    boolean ignore;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {

      if (ignore) {
        return;
      }

      final byte[] buffer = (byte[])msg;

      Tuple request = Tuple.getTuple(buffer, 0, buffer.length);
      switch (request.getType()) {
        case PUBLISHER_REQUEST:

          /*
           * unregister the unidentified client since its job is done!
           */
          logger.info("Received publisher request: {}", request);
          PublishRequestTuple publisherRequest = (PublishRequestTuple)request;

          DataList dl = handlePublisherRequest(publisherRequest, ctx);
          dl.setAutoFlushExecutor(serverHelperExecutor);

          Publisher publisher = new Publisher(dl, (long)request.getBaseSeconds() << 32 | request.getWindowId());

          ctx.pipeline().replace(this, "publisher", publisher);

          ignore = true;

          break;

        case SUBSCRIBER_REQUEST:
          /*
           * unregister the unidentified client since its job is done!
           */
          ignore = true;
          logger.info("Received subscriber request: {}", request);

          SubscribeRequestTuple subscriberRequest = (SubscribeRequestTuple)request;

//          /* for backward compatibility - set the buffer size to 16k - EXPERIMENTAL */
          int bufferSize = subscriberRequest.getBufferSize();
//          if (bufferSize == 0) {
//            bufferSize = 16 * 1024;
//          }
          Subscriber subscriber = new Subscriber(subscriberRequest.getStreamType(), subscriberRequest.getMask(),
              subscriberRequest.getPartitions(), bufferSize);

          ctx.pipeline().addLast(subscriber);

          final LogicalNode logicalNode = handleSubscriberRequest(subscriberRequest, ctx);
          break;

        case PURGE_REQUEST:
          logger.info("Received purge request: {}", request);
          try {
            handlePurgeRequest((PurgeRequestTuple)request, ctx);
          } catch (IOException io) {
            throw new RuntimeException(io);
          }
          break;

        case RESET_REQUEST:
          logger.info("Received reset all request: {}", request);
          try {
            handleResetRequest((ResetRequestTuple)request, ctx);
          } catch (IOException io) {
            throw new RuntimeException(io);
          }
          break;

        default:
          throw new RuntimeException("unexpected message: " + request.toString());
      }
    }

  }

  private class Subscriber extends ChannelHandlerAdapter
  {
    private final String type;
    private final int mask;
    private final int[] partitions;

    Subscriber(String type, int mask, int[] partitions, int bufferSize)
    {
      //super(1024, bufferSize);
      this.type = type;
      this.mask = mask;
      this.partitions = partitions;
      //super.write = false;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
      logger.warn("Received data when no data is expected: {}", msg);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception
    {
      super.channelUnregistered(ctx);
      teardown(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
      teardown(ctx);
      super.exceptionCaught(ctx, cause);
    }

    @Override
    public String toString()
    {
      return "Server.Subscriber{" + "type=" + type + ", mask=" + mask +
          ", partitions=" + (partitions == null ? "null" : Arrays.toString(partitions)) + '}';
    }

    private volatile boolean torndown;

    private void teardown(ChannelHandlerContext ctx)
    {
      //logger.debug("Teardown is being called {}", torndown, new Exception());
      if (torndown) {
        return;
      }
      torndown = true;

      LogicalNode ln = subscriberGroups.get(type);
      if (ln != null) {
        if (subscriberChannels.containsValue(ctx.channel())) {
          final Iterator<Entry<String, Channel>> i = subscriberChannels.entrySet().iterator();
          while (i.hasNext()) {
            if (i.next().getValue() == ctx.channel()) {
              i.remove();
              break;
            }
          }
        }

        ln.removeChannel(ctx);
        if (ln.getPhysicalNodeCount() == 0) {
          DataList dl = publisherBuffers.get(ln.getUpstream());
          if (dl != null) {
            dl.removeDataListener(ln);
          }
          subscriberGroups.remove(ln.getGroup());
        }
        ln.getIterator().close();
      }
    }

  }

  /**
   * When the publisher connects to the server and starts publishing the data,
   * this is the end on the server side which handles all the communication.
   *
   */
  private class Publisher extends ChannelHandlerAdapter
  {
    private final DataList datalist;
    boolean dirty;
    byte[] buffer;
    int position;

    Publisher(DataList dl, long windowId)
    {
      this.datalist = dl;
      buffer = dl.getBuffer(windowId);
      position = dl.getPosition();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
      byte[] data = (byte[])msg;

      if (position + data.length + 5 > buffer.length) {
        datalist.flush(position);
        if (!switchToNewBufferOrSuspendRead(ctx)) {
          return;
        }
      }
      position = VarInt.write(data.length, buffer, position);
      System.arraycopy(data, 0, buffer, position, data.length);
      position += data.length;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception
    {
      datalist.flush(position);
    }

    private boolean switchToNewBufferOrSuspendRead(ChannelHandlerContext ctx)
    {
      if (switchToNewBuffer()) {
        return true;
      }
      datalist.suspendRead(ctx);
      return false;
    }

    private boolean switchToNewBuffer()
    {
      if (datalist.isMemoryBlockAvailable()) {
        buffer = datalist.newBuffer();
        position = 0;
        datalist.addBuffer(buffer);
        return true;
      }
      return false;
    }

    @Override
    public String toString()
    {
      return getClass().getName() + '@' + Integer.toHexString(hashCode()) + " {datalist=" + datalist + '}';
    }

    private volatile boolean torndown;

    private void teardown(ChannelHandlerContext ctx)
    {
      //logger.debug("Teardown is being called {}", torndown, new Exception());
      if (torndown) {
        return;
      }
      torndown = true;

      /*
       * if the publisher unregistered, all the downstream guys are going to be unregistered anyways
       * in our world. So it makes sense to kick them out proactively. Otherwise these clients since
       * are not being written to, just stick around till the next publisher shows up and eat into
       * the data it's publishing for the new subscribers.
       */

      /**
       * since the publisher server died, the queue which it was using would stop pumping the data unless
       * a new publisher comes up with the same name. We leave it to the stream to decide when to bring up a new node
       * with the same identifier as the one which just died.
       */
      if (publisherChannels.containsValue(ctx.channel())) {
        final Iterator<Entry<String, Channel>> i = publisherChannels.entrySet().iterator();
        while (i.hasNext()) {
          if (i.next().getValue() == ctx.channel()) {
            i.remove();
            break;
          }
        }
      }

      ArrayList<LogicalNode> list = new ArrayList<LogicalNode>();
      String publisherIdentifier = datalist.getIdentifier();
      Iterator<LogicalNode> iterator = subscriberGroups.values().iterator();
      while (iterator.hasNext()) {
        LogicalNode ln = iterator.next();
        if (publisherIdentifier.equals(ln.getUpstream())) {
          list.add(ln);
        }
      }

      for (LogicalNode ln : list) {
        ln.boot();
      }
    }

  }


  private static final Logger logger = LoggerFactory.getLogger(Server.class);
}
