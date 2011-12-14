package org.apache.hadoop.hbase.client.replication;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

/**
 * Unit testing of ReplicationAdmin
 */
public class TestReplicationAdmin {

  private static final Log LOG =
      LogFactory.getLog(TestReplicationAdmin.class);
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private final String ID_ONE = "1";
  private final String KEY_ONE = "127.0.0.1:2181:/hbase";
  private final String ID_SECOND = "2";
  private final String KEY_SECOND = "127.0.0.1:2181:/hbase2";

  private static ReplicationSourceManager manager;
  private static ReplicationAdmin admin;
  private static AtomicBoolean replicating = new AtomicBoolean(true);

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    admin = new ReplicationAdmin(conf);
    Path oldLogDir = new Path(TEST_UTIL.getTestDir(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    Path logDir = new Path(TEST_UTIL.getTestDir(),
        HConstants.HREGION_LOGDIR_NAME);
    manager = new ReplicationSourceManager(admin.getReplicationZk(), conf,
        new Stoppable() {
          @Override
          public void stop(String why) {}
          @Override
          public boolean isStopped() {return false;}
        }, FileSystem.get(conf), replicating, logDir, oldLogDir);
  }

  /**
   * Simple testing of adding and removing peers, basically shows that
   * all interactions with ZK work
   * @throws Exception
   */
  @Test
  public void testAddRemovePeer() throws Exception {
    assertEquals(0, manager.getSources().size());
    // Add a valid peer
    admin.addPeer(ID_ONE, KEY_ONE);
    // try adding the same (fails)
    try {
      admin.addPeer(ID_ONE, KEY_ONE);
    } catch (IllegalArgumentException iae) {
      // OK!
    }
    assertEquals(1, admin.getPeersCount());
    // Try to remove an inexisting peer
    try {
      admin.removePeer(ID_SECOND);
      fail();
    } catch (IllegalArgumentException iae) {
      // OK!
    }
    assertEquals(1, admin.getPeersCount());
    // Add a second since multi-slave is supported
    try {
      admin.addPeer(ID_SECOND, KEY_SECOND);
    } catch (IllegalStateException iae) {
      fail();
    }
    assertEquals(2, admin.getPeersCount());
    // Remove the first peer we added
    admin.removePeer(ID_ONE);
    assertEquals(1, admin.getPeersCount());
  }
}
