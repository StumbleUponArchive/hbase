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

package org.apache.hadoop.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Contains utility methods for manipulating HBase meta tables.
 * Be sure to call {@link #shutdown()} when done with this class so it closes
 * resources opened during meta processing (ROOT, META, etc.).  Be careful
 * how you use this class.  If used during migrations, be careful when using
 * this class to check whether migration is needed.
 */
public class MetaUtils {
  private static final Log LOG = LogFactory.getLog(MetaUtils.class);
  private final Configuration conf;
  private FileSystem fs;
  private Path rootdir;
  private HLog log;
  private HRegion rootRegion;
  private Map<byte [], HRegion> metaRegions = Collections.synchronizedSortedMap(
    new TreeMap<byte [], HRegion>(Bytes.BYTES_COMPARATOR));

  /** Default constructor
   * @throws IOException e
   */
  public MetaUtils() throws IOException {
    this(HBaseConfiguration.create());
  }

  /**
   * @param conf Configuration
   * @throws IOException e
   */
  public MetaUtils(Configuration conf) throws IOException {
    this.conf = conf;
    conf.setInt("hbase.client.retries.number", 1);
    this.rootRegion = null;
    initialize();
  }

  /**
   * Verifies that DFS is available and that HBase is off-line.
   * @throws IOException e
   */
  private void initialize() throws IOException {
    this.fs = FileSystem.get(this.conf);
    // Get root directory of HBase installation
    this.rootdir = FSUtils.getRootDir(this.conf);
  }

  /**
   * @return the HLog
   * @throws IOException e
   */
  public synchronized HLog getLog() throws IOException {
    if (this.log == null) {
      Path logdir = new Path(this.fs.getHomeDirectory(),
          HConstants.HREGION_LOGDIR_NAME + "_" + System.currentTimeMillis());
      Path oldLogDir = new Path(this.fs.getHomeDirectory(),
          HConstants.HREGION_OLDLOGDIR_NAME);
      this.log = new HLog(this.fs, logdir, oldLogDir, this.conf, null);
    }
    return this.log;
  }

  /**
   * @return HRegion for root region
   * @throws IOException e
   */
  public HRegion getRootRegion() throws IOException {
    if (this.rootRegion == null) {
      openRootRegion();
    }
    return this.rootRegion;
  }

  /**
   * Open or return cached opened meta region
   *
   * @param metaInfo HRegionInfo for meta region
   * @return meta HRegion
   * @throws IOException e
   */
  public HRegion getMetaRegion(HRegionInfo metaInfo) throws IOException {
    HRegion meta = metaRegions.get(metaInfo.getRegionName());
    if (meta == null) {
      meta = openMetaRegion(metaInfo);
      LOG.info("OPENING META " + meta.toString());
      this.metaRegions.put(metaInfo.getRegionName(), meta);
    }
    return meta;
  }

  /**
   * Closes catalog regions if open. Also closes and deletes the HLog. You
   * must call this method if you want to persist changes made during a
   * MetaUtils edit session.
   */
  public void shutdown() {
    if (this.rootRegion != null) {
      try {
        this.rootRegion.close();
      } catch (IOException e) {
        LOG.error("closing root region", e);
      } finally {
        this.rootRegion = null;
      }
    }
    try {
      for (HRegion r: metaRegions.values()) {
        LOG.info("CLOSING META " + r.toString());
        r.close();
      }
    } catch (IOException e) {
      LOG.error("closing meta region", e);
    } finally {
      metaRegions.clear();
    }
    try {
      if (this.log != null) {
        this.log.rollWriter();
        this.log.closeAndDelete();
      }
    } catch (IOException e) {
      LOG.error("closing HLog", e);
    } finally {
      this.log = null;
    }
  }

  /**
   * Used by scanRootRegion and scanMetaRegion to call back the caller so it
   * can process the data for a row.
   */
  public interface ScannerListener {
    /**
     * Callback so client of scanner can process row contents
     *
     * @param info HRegionInfo for row
     * @return false to terminate the scan
     * @throws IOException e
     */
    public boolean processRow(HRegionInfo info) throws IOException;
  }

  /**
   * Scans the root region. For every meta region found, calls the listener with
   * the HRegionInfo of the meta region.
   *
   * @param listener method to be called for each meta region found
   * @throws IOException e
   */
  public void scanRootRegion(ScannerListener listener) throws IOException {
    // Open root region so we can scan it
    if (this.rootRegion == null) {
      openRootRegion();
    }
    scanMetaRegion(this.rootRegion, listener);
  }

  /**
   * Scan the passed in metaregion <code>m</code> invoking the passed
   * <code>listener</code> per row found.
   * @param r region
   * @param listener scanner listener
   * @throws IOException e
   */
  public void scanMetaRegion(final HRegion r, final ScannerListener listener)
  throws IOException {
    Scan scan = new Scan();
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    InternalScanner s = r.getScanner(scan);
    try {
      List<KeyValue> results = new ArrayList<KeyValue>();
      boolean hasNext = true;
      do {
        hasNext = s.next(results);
        HRegionInfo info = null;
        for (KeyValue kv: results) {
          info = Writables.getHRegionInfoOrNull(kv.getValue());
          if (info == null) {
            LOG.warn("Region info is null for row " +
              Bytes.toString(kv.getRow()) + " in table " +
              r.getTableDesc().getNameAsString());
          }
          continue;
        }
        if (!listener.processRow(info)) {
          break;
        }
        results.clear();
      } while (hasNext);
    } finally {
      s.close();
    }
  }

  /**
   * Scans a meta region. For every region found, calls the listener with
   * the HRegionInfo of the region.
   * TODO: Use Visitor rather than Listener pattern.  Allow multiple Visitors.
   * Use this everywhere we scan meta regions: e.g. in metascanners, in close
   * handling, etc.  Have it pass in the whole row, not just HRegionInfo.
   * <p>Use for reading meta only.  Does not close region when done.
   * Use {@link #getMetaRegion(HRegionInfo)} instead if writing.  Adds
   * meta region to list that will get a close on {@link #shutdown()}.
   *
   * @param metaRegionInfo HRegionInfo for meta region
   * @param listener method to be called for each meta region found
   * @throws IOException e
   */
  public void scanMetaRegion(HRegionInfo metaRegionInfo,
    ScannerListener listener)
  throws IOException {
    // Open meta region so we can scan it
    HRegion metaRegion = openMetaRegion(metaRegionInfo);
    scanMetaRegion(metaRegion, listener);
  }

  private synchronized HRegion openRootRegion() throws IOException {
    if (this.rootRegion != null) {
      return this.rootRegion;
    }
    this.rootRegion = HRegion.openHRegion(HRegionInfo.ROOT_REGIONINFO,
      this.rootdir, getLog(), this.conf);
    this.rootRegion.compactStores();
    return this.rootRegion;
  }

  private HRegion openMetaRegion(HRegionInfo metaInfo) throws IOException {
    HRegion meta =
      HRegion.openHRegion(metaInfo, this.rootdir, getLog(), this.conf);
    meta.compactStores();
    return meta;
  }

  /**
   * Set a single region on/offline.
   * This is a tool to repair tables that have offlined tables in their midst.
   * Can happen on occasion.  Use at your own risk.  Call from a bit of java
   * or jython script.  This method is 'expensive' in that it creates a
   * {@link HTable} instance per invocation to go against <code>.META.</code>
   * @param c A configuration that has its <code>hbase.master</code>
   * properly set.
   * @param row Row in the catalog .META. table whose HRegionInfo's offline
   * status we want to change.
   * @param onlineOffline Pass <code>true</code> to OFFLINE the region.
   * @throws IOException e
   */
  public static void changeOnlineStatus (final Configuration c,
      final byte [] row, final boolean onlineOffline)
  throws IOException {
    HTable t = new HTable(c, HConstants.META_TABLE_NAME);
    Get get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    Result res = t.get(get);
    KeyValue [] kvs = res.raw();
    if(kvs.length <= 0) {
      throw new IOException("no information for row " + Bytes.toString(row));
    }
    byte [] value = kvs[0].getValue();
    if (value == null) {
      throw new IOException("no information for row " + Bytes.toString(row));
    }
    HRegionInfo info = Writables.getHRegionInfo(value);
    Put put = new Put(row);
    info.setOffline(onlineOffline);
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(info));
    t.put(put);

    Delete delete = new Delete(row);
    delete.deleteColumns(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
    delete.deleteColumns(HConstants.CATALOG_FAMILY,
        HConstants.STARTCODE_QUALIFIER);

    t.delete(delete);
  }

  /**
   * Offline version of the online TableOperation,
   * org.apache.hadoop.hbase.master.AddColumn.
   * @param tableName table name
   * @param hcd Add this column to <code>tableName</code>
   * @throws IOException e
   */
  public void addColumn(final byte [] tableName,
      final HColumnDescriptor hcd)
  throws IOException {
    List<HRegionInfo> metas = getMETARows(tableName);
    for (HRegionInfo hri: metas) {
      final HRegion m = getMetaRegion(hri);
      scanMetaRegion(m, new ScannerListener() {
        private boolean inTable = true;

        @SuppressWarnings("synthetic-access")
        public boolean processRow(HRegionInfo info) throws IOException {
          LOG.debug("Testing " + Bytes.toString(tableName) + " against " +
            Bytes.toString(info.getTableDesc().getName()));
          if (Bytes.equals(info.getTableDesc().getName(), tableName)) {
            this.inTable = false;
            info.getTableDesc().addFamily(hcd);
            updateMETARegionInfo(m, info);
            return true;
          }
          // If we got here and we have not yet encountered the table yet,
          // inTable will be false.  Otherwise, we've passed out the table.
          // Stop the scanner.
          return this.inTable;
        }});
    }
  }

  public void setVersionReplicationCompression(final byte [] tableName)
  throws IOException {
    List<HRegionInfo> metas = getMETARows(tableName);
    for (HRegionInfo hri: metas) {
      final HRegion m = getMetaRegion(hri);
      scanMetaRegion(m, new ScannerListener() {
        private boolean inTable = true;

        @SuppressWarnings("synthetic-access")
        public boolean processRow(HRegionInfo info) throws IOException {
          LOG.debug("Testing " + Bytes.toString(tableName) + " against " +
            Bytes.toString(info.getTableDesc().getName()));
          if (Bytes.equals(info.getTableDesc().getName(), tableName)) {
            this.inTable = false;
            HColumnDescriptor fam = info.getTableDesc().getColumnFamilies()[0];
            fam.setMaxVersions(1);
            fam.setCompressionType(Compression.Algorithm.LZO);
            fam.setScope(1);
            updateMETARegionInfo(m, info);
            return true;
          }
          // If we got here and we have not yet encountered the table yet,
          // inTable will be false.  Otherwise, we've passed out the table.
          // Stop the scanner.
          return this.inTable;
        }});
    }
  }

  /**
   * Offline version of the online TableOperation,
   * org.apache.hadoop.hbase.master.DeleteColumn.
   * @param tableName table name
   * @param columnFamily Name of column name to remove.
   * @throws IOException e
   */
  public void deleteColumn(final byte [] tableName,
      final byte [] columnFamily) throws IOException {
    List<HRegionInfo> metas = getMETARows(tableName);
    for (HRegionInfo hri: metas) {
      final HRegion m = getMetaRegion(hri);
      scanMetaRegion(m, new ScannerListener() {
        private boolean inTable = true;

        @SuppressWarnings("synthetic-access")
        public boolean processRow(HRegionInfo info) throws IOException {
          if (Bytes.equals(info.getTableDesc().getName(), tableName)) {
            this.inTable = false;
            info.getTableDesc().removeFamily(columnFamily);
            updateMETARegionInfo(m, info);
            Path tabledir = new Path(rootdir,
              info.getTableDesc().getNameAsString());
            Path p = Store.getStoreHomedir(tabledir, info.getEncodedName(),
              columnFamily);
            if (!fs.delete(p, true)) {
              LOG.warn("Failed delete of " + p);
            }
            return false;
          }
          // If we got here and we have not yet encountered the table yet,
          // inTable will be false.  Otherwise, we've passed out the table.
          // Stop the scanner.
          return this.inTable;
        }});
    }
  }

  /**
   * Update COL_REGIONINFO in meta region r with HRegionInfo hri
   *
   * @param r region
   * @param hri region info
   * @throws IOException e
   */
  public void updateMETARegionInfo(HRegion r, final HRegionInfo hri)
  throws IOException {
    if (LOG.isDebugEnabled()) {
      Get get = new Get(hri.getRegionName());
      get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      Result res = r.get(get, null);
      KeyValue [] kvs = res.raw();
      if(kvs.length <= 0) {
        return;
      }
      byte [] value = kvs[0].getValue();
      if (value == null) {
        return;
      }
      HRegionInfo h = Writables.getHRegionInfoOrNull(value);

      LOG.debug("Old " + Bytes.toString(HConstants.CATALOG_FAMILY) + ":" +
          Bytes.toString(HConstants.REGIONINFO_QUALIFIER) + " for " +
          hri.toString() + " in " + r.toString() + " is: " + h.toString());
    }

    Put put = new Put(hri.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(hri));
    r.put(put);

    if (LOG.isDebugEnabled()) {
      Get get = new Get(hri.getRegionName());
      get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      Result res = r.get(get, null);
      KeyValue [] kvs = res.raw();
      if(kvs.length <= 0) {
        return;
      }
      byte [] value = kvs[0].getValue();
      if (value == null) {
        return;
      }
      HRegionInfo h = Writables.getHRegionInfoOrNull(value);
        LOG.debug("New " + Bytes.toString(HConstants.CATALOG_FAMILY) + ":" +
            Bytes.toString(HConstants.REGIONINFO_QUALIFIER) + " for " +
            hri.toString() + " in " + r.toString() + " is: " +  h.toString());
    }
  }

  /**
   * @return List of {@link HRegionInfo} rows found in the ROOT or META
   * catalog table.
   * @param tableName Name of table to go looking for.
   * @throws IOException e
   * @see #getMetaRegion(HRegionInfo)
   */
  public List<HRegionInfo> getMETARows(final byte [] tableName)
  throws IOException {
    final List<HRegionInfo> result = new ArrayList<HRegionInfo>();
    // If passed table name is META, then  return the root region.
    if (Bytes.equals(HConstants.META_TABLE_NAME, tableName)) {
      result.add(openRootRegion().getRegionInfo());
      return result;
    }
    // Return all meta regions that contain the passed tablename.
    scanRootRegion(new ScannerListener() {
      private final Log SL_LOG = LogFactory.getLog(this.getClass());

      public boolean processRow(HRegionInfo info) throws IOException {
        SL_LOG.debug("Testing " + info);
        if (Bytes.equals(info.getTableDesc().getName(),
            HConstants.META_TABLE_NAME)) {
          result.add(info);
          return false;
        }
        return true;
      }});
    return result;
  }

  /**
   * @param n Table name.
   * @return True if a catalog table, -ROOT- or .META.
   */
  public static boolean isMetaTableName(final byte [] n) {
    return Bytes.equals(n, HConstants.ROOT_TABLE_NAME) ||
      Bytes.equals(n, HConstants.META_TABLE_NAME);
  }

  public static void main(String[] args) throws Exception {
    MetaUtils utils = new MetaUtils(HBaseConfiguration.create());
    utils.setVersionReplicationCompression(Bytes.toBytes(args[0]));
    utils.shutdown();
  }
}
