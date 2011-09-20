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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test that the actions are called while playing with an HLog
 */
public class TestWALObserver {
  protected static final Log LOG = LogFactory.getLog(TestWALObserver.class);

  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private final static byte[] SOME_BYTES =  Bytes.toBytes("t");
  private static FileSystem fs;
  private static Path oldLogDir;
  private static Path logDir;
  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.regionserver.maxlogs", 5);
    conf.setInt("dfs.datanode.socket.write.timeout", 5000);
    conf.setInt("dfs.socket.timeout", 5000);
    conf.setInt("ipc.ping.interval", 5000);
    //fs = FileSystem.get(conf);
//    oldLogDir = new Path(HBaseTestingUtility.getTestDir(),
//        HConstants.HREGION_OLDLOGDIR_NAME);
//    logDir = new Path(HBaseTestingUtility.getTestDir(),
//        HConstants.HREGION_LOGDIR_NAME);
    File clusterTestBuildDir = TEST_UTIL.setupClusterTestBuildDir();
    TEST_UTIL.startMiniDFSCluster(3, clusterTestBuildDir);
    MiniDFSCluster cluster = TEST_UTIL.getDFSCluster();
    fs = cluster.getFileSystem();
    Path hbaseDir =TEST_UTIL.createRootDir();
    oldLogDir = new Path(hbaseDir, ".oldlogs");
    logDir = new Path(hbaseDir, "TestWALObserver");
  }

  @Before
  public void setUp() throws Exception {
    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      fs.delete(dir.getPath(), true);
    }
  }

  @After
  public void tearDown() throws Exception {
    setUp();
  }

  /**
   * Add a bunch of dummy data and roll the logs every two insert. We
   * should end up with 10 rolled files (plus the roll called in
   * the constructor). Also test adding a listener while it's running.
   */
  @Test
  public void testActionListener() throws Exception {
    DummyWALObserver observer = new DummyWALObserver();
    List<WALObserver> list = new ArrayList<WALObserver>();
    list.add(observer);
    DummyWALObserver laterobserver = new DummyWALObserver();
    HLog hlog = new HLog(fs, logDir, oldLogDir, conf, list, null);
    //LOG.debug(System.currentTimeMillis());
    //hlog.sync();
    //LOG.debug(System.currentTimeMillis());
    Thread.sleep(10000);
    hlog.sync();
    /*HRegionInfo hri = new HRegionInfo(new HTableDescriptor(SOME_BYTES),
        SOME_BYTES, SOME_BYTES, false);

    for (int i = 0; i < 20; i++) {
      byte[] b = Bytes.toBytes(i+"");
      KeyValue kv = new KeyValue(b,b,b);
      WALEdit edit = new WALEdit();
      edit.add(kv);
      HLogKey key = new HLogKey(b,b, 0, 0);
      hlog.append(hri, key, edit);
      if (i == 10) {
        hlog.registerWALActionsListener(laterobserver);
      }
      if (i % 2 == 0) {
        hlog.rollWriter();
      }
    }*/

    hlog.close();
    hlog.closeAndDelete();

    /*assertEquals(11, observer.logRollCounter);
    assertEquals(5, laterobserver.logRollCounter);
    assertEquals(2, observer.closedCount);*/
  }

  /**
   * Just counts when methods are called
   */
  static class DummyWALObserver implements WALObserver {
    public int logRollCounter = 0;
    public int closedCount = 0;

    @Override
    public void logRolled(Path newFile) {
      logRollCounter++;
    }

    @Override
    public void logRollRequested() {
      // Not interested
    }

    @Override
    public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey,
        WALEdit logEdit) {
      // Not interested
      
    }

    @Override
    public void logCloseRequested() {
      closedCount++;
    }
  }
}