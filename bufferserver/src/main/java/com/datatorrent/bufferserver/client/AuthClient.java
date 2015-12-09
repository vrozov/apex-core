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

import java.net.SocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * <p>Auth Client class.</p>
 *
 * @since 3.0.0
 */
public abstract class AuthClient extends ChannelHandlerAdapter
{
  private Channel channel;
  private byte[] token;

  public AuthClient()
  {
  }

  public ChannelFuture connect(EventLoopGroup eventLoopGroup, SocketAddress remoteAddress) {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(eventLoopGroup)
        .channel(NioSocketChannel.class)
        .handler(
            new ChannelInitializer<SocketChannel>()
            {
              @Override
              public void initChannel(SocketChannel ch) throws Exception
              {
                final ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("Logger", new LoggingHandler(LogLevel.TRACE));
                pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                pipeline.addLast("bytesDecoder", new ByteArrayDecoder());
                pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
                pipeline.addLast("bytesEncoder", new ByteArrayEncoder());
                pipeline.addLast(AuthClient.this);
              }
            }
        );
    return bootstrap.connect(remoteAddress);
  }

  public void activate(ChannelFuture channelFuture)
  {
    try {
      channel = channelFuture.channel();
      channelFuture.sync();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public ChannelFuture write(byte[] msg) {
    return channel.write(msg, channel.voidPromise());
  }

  public ChannelFuture disconnect() {
    return channel.disconnect();
  }

  public void flush() {
    channel.flush();
  }

  public void suspendRead() {
    channel.config().setAutoRead(false);
  }

  public void resumeRead() {
    channel.config().setAutoRead(true);
  }

  public void setToken(byte[] token)
  {
    this.token = token;
  }
}
