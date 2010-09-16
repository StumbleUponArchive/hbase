/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client.replication;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationZookeeperWrapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;

/**
 * This class provides the administrative interface to HBase cluster
 * replication. In order to use it, the cluster and the client using
 * ReplicationAdmin must be configured with <code>hbase.replication</code>
 * set to true.
 *
 * Adding a new peer results in creating new outbound connections from every
 * region server to a subset of region servers on the slave cluster. Each
 * new stream of replication will start replicating from the beginning of the
 * current HLog, meaning that edits from that past will be replicated.
 *
 * Removing a peer is a destructive and irreversible operation that stops
 * all the replication streams for the given cluster and deletes the metadata
 * used to keep track of the replication state.
 *
 * Enabling and disabling peers is currently not supported.
 *
 * As cluster replication is still experimental, a kill switch is provided
 * in order to stop all replication-related operations, see
 * {@link #setReplicating(boolean)}. When setting it back to true, the new
 * state of all the replication streams will be unknown and may have holes.
 * Use at your own risk.
 *
 * To see which commands are available in the shell, type
 * <code>replication</code>.
 */
public class ReplicationAdmin {

  private final Configuration conf;
  private final ReplicationZookeeperWrapper zkWrapper;

  /**
   * Constructor that creates a connection to the local ZooKeeper ensemble.
   * @param conf Configuration to use
   * @throws IOException if the connection to ZK cannot be made
   * @throws RuntimeException if replication isn't enabled.
   */
  public ReplicationAdmin(Configuration conf) throws IOException {
    if (!conf.getBoolean(HConstants.REPLICATION_ENABLE_KEY, false)) {
      throw new RuntimeException("hbase.replication isn't true, please " +
          "enable it in order to use replication");
    }
    this.conf = conf;
    ZooKeeperWrapper zkw = HConnectionManager.getConnection(conf).
        getZooKeeperWrapper();
    zkWrapper = new ReplicationZookeeperWrapper(zkw, conf);
  }

  /**
   * Add a new peer cluster to replicate to.
   * @param id a short that identifies the cluster
   * @param clusterKey the concatenation of the slave cluster's
   * <code>hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent</code>
   * @throws IllegalStateException if there's already one slave since
   * multi-slave isn't supported yet.
   */
  public void addPeer(String id, String clusterKey) {
    this.zkWrapper.addPeer(id, clusterKey);
  }

  /**
   * Remove a peer cluster, stops the replication to.
   * @param id a short that identifies the cluster
   */
  public void removePeer(String id) {
    this.zkWrapper.removePeer(id);
  }

  /**
   * Restart the replication stream to the specified peer
   * @param id a short that identifies the cluster
   */
  public void enablePeer(String id) {
    throw new RuntimeException("Not implemented");
  }

  /**
   * Stop the replication stream to the specified peer
   * @param id a short that identifies the cluster
   */
  public void disablePeer(String id) {
    throw new RuntimeException("Not implemented");
  }

  /**
   * Get the number of slave clusters the local cluster has
   * @return
   */
  public int getPeersCount() {
    return this.zkWrapper.listPeersIds(null).size();
  }

  /**
   * Kill switch for all replication-related features
   * @param newState true to start replication, false to stop it
   * completely
   */
  public void setReplicating(boolean newState) {
    this.zkWrapper.setReplicating(newState);
  }

  /**
   * Get the ZK wrapper created and used by this object
   * @return
   */
  public ReplicationZookeeperWrapper getZkWrapper() {
    return zkWrapper;
  }

  public long verifyReplication(String id, String tableName, long startTime) throws Exception {
    if (startTime <= 60000) {
      throw new IllegalArgumentException("start time must be bigger than 1 minute (60000)");
    }
    startTime = System.currentTimeMillis() - startTime;
    long stopTime = System.currentTimeMillis() - 60000;
    ReplicationPeer peer = this.zkWrapper.getPeer(id);
    HTable ourTable = new HTable(tableName);
    HTable replicatedTable = new HTable(peer.getConfiguration(), tableName);
    Scan ourScan = new Scan();
    ourScan.setCaching(30);
    ourScan.setTimeRange(startTime, stopTime);
    ResultScanner ourScanner = ourTable.getScanner(ourScan);
    ResultScanner replicatedScanner = replicatedTable.getScanner(new Scan(ourScan));
    long count = 0;
    for (Result ourResult : ourScanner) {
      Result replicatedResult = replicatedScanner.next();
      compareResult(ourResult, replicatedResult);
      count++;
    }
    if (replicatedScanner.next() != null) {
      throw new Exception("The slave has more rows than us, we only had " + count + " rows");
    }
    return count;
  }

  public static void compareResult(Result res1, Result res2) throws Exception{
    if (res2 == null) {
      throw new Exception("There wasn't enough rows, we stopped at" + Bytes.toString(res1.getRow()));
    }
    if (res1.size() != res2.size()) {
      throw new Exception("This row doesn't have the same number of KVs: " + res1.toString() + " compared to " + res2.toString());
    }
    KeyValue[] ourKVs = res1.sorted();
    KeyValue[] replicatedKVs = res2.sorted();
    for (int i = 0; i < res1.size(); i++) {
      if (!ourKVs[i].equals(replicatedKVs[i]) && !Bytes.equals(ourKVs[i].getValue(), replicatedKVs[i].getValue())) {
        throw new Exception("This result was different: " + res1.toString() + " compared to " + res2.toString());
      }
    }
  }
}
