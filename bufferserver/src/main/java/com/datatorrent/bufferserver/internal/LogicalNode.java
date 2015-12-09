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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.internal.DataList.DataListIterator;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.bufferserver.policy.GiveAll;
import com.datatorrent.bufferserver.policy.Policy;
import com.datatorrent.bufferserver.util.BitVector;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.bufferserver.util.SerializedData;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * LogicalNode represents a logical node in a DAG<p>
 * <br>
 * Logical node can be split into multiple physical nodes. The type of the logical node groups the multiple
 * physical nodes together in a group.
 * <br>
 *
 * @since 0.3.2
 */
public class LogicalNode implements DataListener
{
  private final String upstream;
  private final String group;
  private final HashSet<PhysicalNode> physicalNodes;
  private final HashSet<BitVector> partitions;
  private final Policy policy = GiveAll.getInstance();
  private final DataListIterator iterator;
  private final long skipWindowId;
  private long baseSeconds;
  private boolean caughtup;

  /**
   *
   * @param upstream
   * @param group
   * @param iterator
   * @param skipUptoWindowId
   */
  public LogicalNode(String upstream, String group, Iterator<SerializedData> iterator, long skipUptoWindowId)
  {
    this.upstream = upstream;
    this.group = group;
    this.physicalNodes = new HashSet<PhysicalNode>();
    this.partitions = new HashSet<BitVector>();

    if (iterator instanceof DataListIterator) {
      this.iterator = (DataListIterator)iterator;
    } else {
      throw new IllegalArgumentException("iterator does not belong to DataListIterator class");
    }

    skipWindowId = skipUptoWindowId;
  }

  /**
   *
   * @return String
   */
  public String getGroup()
  {
    return group;
  }

  /**
   *
   * @return Iterator<SerializedData>
   */
  public Iterator<SerializedData> getIterator()
  {
    return iterator;
  }

  /**
   *
   * @param connection
   */
  public void addConnection(final Channel channel)
  {
    PhysicalNode pn = new PhysicalNode(channel);
    if (!physicalNodes.contains(pn)) {
      physicalNodes.add(pn);
    }
  }

  /**
   *
   * @param client
   */
  public void removeChannel(final ChannelHandlerContext ctx)
  {
    for (PhysicalNode pn : physicalNodes) {
      if (pn.getClient() == ctx) {
        physicalNodes.remove(pn);
        break;
      }
    }
  }

  /**
   *
   * @param partition
   * @param mask
   */
  public void addPartition(int partition, int mask)
  {
    partitions.add(new BitVector(partition, mask));
  }

  boolean ready = true;

  public boolean isReady()
  {
    if (!ready) {
      ready = true;
      for (PhysicalNode pn : physicalNodes) {
        if (pn.isBlocked()) {
          ready = pn.unblock() & ready;
        }
      }
    }

    return ready;
  }

  // make it run a lot faster by tracking faster!
  /**
   *
   */
  public void catchUp()
  {
    long lBaseSeconds = (long)iterator.getBaseSeconds() << 32;
    logger.debug("BaseSeconds = {} and lBaseSeconds = {}", Codec.getStringWindowId(baseSeconds),
        Codec.getStringWindowId(lBaseSeconds));
    if (lBaseSeconds > baseSeconds) {
      baseSeconds = lBaseSeconds;
    }
    logger.debug("Set the base seconds to {}", Codec.getStringWindowId(baseSeconds));
    int intervalMillis;

    int skippedPayloadTuples = 0;

    if (isReady()) {
      logger.debug("catching up {}->{}", upstream, group);
      try {
        /*
         * fast forward to catch up with the windowId without consuming
         */
        outer:
        while (ready && iterator.hasNext()) {
          SerializedData data = iterator.next();
          switch (data.buffer[data.dataOffset]) {

            case MessageType.PAYLOAD_VALUE:
              ++skippedPayloadTuples;
              break;

            case MessageType.RESET_WINDOW_VALUE:
              Tuple tuple = Tuple.getTuple(data.buffer, data.dataOffset, data.length - data.dataOffset + data.offset);
              baseSeconds = (long)tuple.getBaseSeconds() << 32;
              intervalMillis = tuple.getWindowWidth();
              if (intervalMillis <= 0) {
                logger.warn("Interval value set to non positive value = {}", intervalMillis);
              }
              ready = GiveAll.getInstance().distribute(physicalNodes, data);
              break;

            case MessageType.BEGIN_WINDOW_VALUE:
              tuple = Tuple.getTuple(data.buffer, data.dataOffset, data.length - data.dataOffset + data.offset);
              logger.debug("{}->{} condition {} =? {}", upstream, group,
                  Codec.getStringWindowId(baseSeconds | tuple.getWindowId()), Codec.getStringWindowId(skipWindowId));
              if ((baseSeconds | tuple.getWindowId()) > skipWindowId) {
                logger.debug("caught up {}->{} skipping {} payload tuples", upstream, group, skippedPayloadTuples);
                ready = GiveAll.getInstance().distribute(physicalNodes, data);
                caughtup = true;
                break outer;
              }
              break;

            case MessageType.CHECKPOINT_VALUE:
            case MessageType.CODEC_STATE_VALUE:
            case MessageType.END_STREAM_VALUE:
              ready = GiveAll.getInstance().distribute(physicalNodes, data);
              logger.debug("Message {} was distributed to {}", MessageType.valueOf(data.buffer[data.dataOffset]),
                  physicalNodes);
              break;
            default:
              logger.debug("Message {} was not distributed to {}", MessageType.valueOf(data.buffer[data.dataOffset]),
                  physicalNodes);
          }
        }
        if (ready)
        {
          for (PhysicalNode physicalNode : physicalNodes) {
            physicalNode.getClient().flush();
          }
        }
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }

      if (iterator.hasNext()) {
        addedData();
      }
    }

    logger.debug("Exiting catch up because caughtup = {}", caughtup);
  }

  @Override
  public boolean addedData()
  {
    if (isReady()) {
      if (caughtup) {
        try {
          /*
           * consume as much data as you can before running out of steam
           */
          if (partitions.isEmpty()) {
            while (ready && iterator.hasNext()) {
              SerializedData data = iterator.next();
              switch (data.buffer[data.dataOffset]) {
                case MessageType.PAYLOAD_VALUE:
                  ready = policy.distribute(physicalNodes, data);
                  break;

                case MessageType.NO_MESSAGE_VALUE:
                case MessageType.NO_MESSAGE_ODD_VALUE:
                  break;

                case MessageType.RESET_WINDOW_VALUE:
                  final int length = data.length - data.dataOffset + data.offset;
                  Tuple resetWindow = Tuple.getTuple(data.buffer, data.dataOffset, length);
                  baseSeconds = (long)resetWindow.getBaseSeconds() << 32;
                  ready = GiveAll.getInstance().distribute(physicalNodes, data);
                  break;

                default:
                  //logger.debug("sending data of type {}", MessageType.valueOf(data.buffer[data.dataOffset]));
                  ready = GiveAll.getInstance().distribute(physicalNodes, data);
                  break;
              }
            }
            if (ready) {
              for (PhysicalNode node : physicalNodes) {
                node.getClient().flush();
              }
            }

          } else {
            while (ready && iterator.hasNext()) {
              SerializedData data = iterator.next();
              final int length = data.length - data.dataOffset + data.offset;
              switch (data.buffer[data.dataOffset]) {
                case MessageType.PAYLOAD_VALUE:
                  Tuple tuple = Tuple.getTuple(data.buffer, data.dataOffset, length);
                  int value = tuple.getPartition();
                  for (BitVector bv : partitions) {
                    if (bv.matches(value)) {
                      ready = policy.distribute(physicalNodes, data);
                      break;
                    }
                  }
                  break;

                case MessageType.NO_MESSAGE_VALUE:
                case MessageType.NO_MESSAGE_ODD_VALUE:
                  break;

                case MessageType.RESET_WINDOW_VALUE:
                  tuple = Tuple.getTuple(data.buffer, data.dataOffset, length);
                  baseSeconds = (long)tuple.getBaseSeconds() << 32;
                  ready = GiveAll.getInstance().distribute(physicalNodes, data);
                  break;

                default:
                  ready = GiveAll.getInstance().distribute(physicalNodes, data);
                  break;
              }
            }
            if (ready) {
              for (PhysicalNode node : physicalNodes) {
                node.getClient().flush();
              }
            }
          }
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
      } else {
        catchUp();
      }
    }
    return !ready;
  }

  /**
   *
   * @param partitions
   * @return int
   */
  @Override
  public int getPartitions(Collection<BitVector> partitions)
  {
    partitions.addAll(this.partitions);
    return partitions.size();
  }

  /**
   *
   * @return int
   */
  public final int getPhysicalNodeCount()
  {
    return physicalNodes.size();
  }

  /**
   * @return the upstream
   */
  public String getUpstream()
  {
    return upstream;
  }

  public void boot()
  {
    for (PhysicalNode pn : physicalNodes) {
      pn.getClient().disconnect();
    }
    physicalNodes.clear();
  }

  @Override
  public String toString()
  {
    return "LogicalNode{" + "upstream=" + upstream + ", group=" + group + ", partitions=" + partitions +
        ", iterator=" + iterator + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(LogicalNode.class);
}
