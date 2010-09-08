package org.apache.hadoop.hbase.client.replication;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.replication.ReplicationZookeeperWrapper;
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
}
