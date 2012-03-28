/**
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

package org.apache.hadoop.hbase.thrift;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.Increment;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.ScanResult;
import org.apache.hadoop.hbase.thrift.generated.ScanSpec;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRegionInfo;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.thrift.generated.TScan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.net.DNS;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.jruby.threading.DaemonThreadFactory;

import static org.apache.hadoop.hbase.util.Bytes.getBytes;

/**
 * ThriftServer - this class starts up a Thrift server which implements the
 * Hbase API specified in the Hbase.thrift IDL file.
 */
public class ThriftServer {

  public interface HBaseHandlerMBean {
    public int getQueueSize();

    public int getFailQueueSize();

    public void setFailQueueSize(int newSize);

    public int getScannerMapSize();

    public int getNextScannerId();

    public long getPoolCompletedTaskCount();

    public long getPoolTaskCount();

    public int getPoolLargestPoolSize();

    public int getCorePoolSize();

    public void setCorePoolSize(int newCoreSize);

    public int getMaxPoolSize();

    public void setMaxPoolSize(int newMaxSize);

    public long getFailedIncrements();

    public long getSuccessfulCoalescings();

    public long getTotalIncrements();

    public long getCountersMapSize();
  }

  /**
   * The HBaseHandler is a glue object that connects Thrift RPC calls to the
   * HBase client API primarily defined in the HBaseAdmin and HTable objects.
   */
  public static class HBaseHandler implements Hbase.Iface, HBaseHandlerMBean {

    // MBean get/set methods
    public int getQueueSize() {
      return pool.getQueue().size();
    }
    public int getFailQueueSize() {
      return this.failQueueSize;
    }
    public void setFailQueueSize(int newSize) {
      this.failQueueSize = newSize;
    }
    public int getScannerMapSize() {
      return scannerMap.size();
    }
    public int getNextScannerId() {
      return nextScannerId.get();
    }
    public long getPoolCompletedTaskCount() {
      return pool.getCompletedTaskCount();
    }
    public long getPoolTaskCount() {
      return pool.getTaskCount();
    }
    public int getPoolLargestPoolSize() {
      return pool.getLargestPoolSize();
    }
    public int getCorePoolSize() {
      return pool.getCorePoolSize();
    }
    public void setCorePoolSize(int newCoreSize) {
      pool.setCorePoolSize(newCoreSize);
    }
    public int getMaxPoolSize() {
      return pool.getMaximumPoolSize();
    }
    public void setMaxPoolSize(int newMaxSize) {
      pool.setMaximumPoolSize(newMaxSize);
    }
    public long getFailedIncrements() {
      return failedIncrements.get();
    }

    public long getSuccessfulCoalescings() {
      return successfulCoalescings.get();
    }

    public long getTotalIncrements() {
      return totalIncrements.get();
    }

    public long getCountersMapSize() {
      return countersMap.size();
    }

    private Stoppable STOPPABLE = new Stoppable() {
      final AtomicBoolean stop = new AtomicBoolean(false);

      @Override
      public boolean isStopped() {
        return this.stop.get();
      }

      @Override
      public void stop(String why) {
        LOG.info("STOPPING BECAUSE: " + why);
        this.stop.set(true);
      }

    };

    static class DaemonThreadFactory implements ThreadFactory {
      static final AtomicInteger poolNumber = new AtomicInteger(1);
      final ThreadGroup group;
      final AtomicInteger threadNumber = new AtomicInteger(1);
      final String namePrefix;

      DaemonThreadFactory() {
        SecurityManager s = System.getSecurityManager();
        group = (s != null)? s.getThreadGroup() :
            Thread.currentThread().getThreadGroup();
        namePrefix = "ICV-" +
            poolNumber.getAndIncrement() +
            "-thread-";
      }

      public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r,
            namePrefix + threadNumber.getAndIncrement(),
            0);
        if (!t.isDaemon())
          t.setDaemon(true);
        if (t.getPriority() != Thread.NORM_PRIORITY)
          t.setPriority(Thread.NORM_PRIORITY);
        return t;
      }
    }

    protected Configuration conf;
    protected HBaseAdmin admin = null;
    protected final Log LOG = LogFactory.getLog(this.getClass().getName());

    static class ScannerAndNext {
      final ResultScanner scanner;
      final Result next;
      final long lastTimeUsed;

      public ScannerAndNext(ResultScanner scanner,
                            Result next) {
        this.scanner = scanner;
        this.next = next;
        this.lastTimeUsed = System.currentTimeMillis();
      }
    }

    protected Map<Integer, ScannerAndNext> newScannerMap =
       new ConcurrentHashMap<Integer,ScannerAndNext>();

    // nextScannerId and scannerMap are used to manage scanner state
    protected AtomicInteger nextScannerId = new AtomicInteger();
    protected Map<Integer, ResultScanner> scannerMap =
        new ConcurrentHashMap<Integer, ResultScanner>();

    private static ThreadLocal<Map<String, HTable>> threadLocalTables = new ThreadLocal<Map<String, HTable>>() {
      @Override
      protected Map<String, HTable> initialValue() {
        return new TreeMap<String, HTable>();
      }
    };

    private final ThreadPoolExecutor pool;

    private int failQueueSize = 500000;
    private static final int CORE_POOL_SIZE = 1;

    private final AtomicLong failedIncrements = new AtomicLong();
    private final AtomicLong successfulCoalescings = new AtomicLong();
    private final AtomicLong totalIncrements = new AtomicLong();

    // For the initialSize we set as high as it should normally reach
    // The loadFactor is default
    // The concurrencyLevel is set to the number of apache workers
    private final ConcurrentMap<KeyValue, Long> countersMap =
        new ConcurrentHashMap<KeyValue, Long>(100000, 0.75f, 1500);

    /**
     * Returns a list of all the column families for a given htable.
     *
     * @param table
     * @return
     * @throws IOException
     */
    byte[][] getAllColumns(HTable table) throws IOException {
      HColumnDescriptor[] cds = table.getTableDescriptor().getColumnFamilies();
      byte[][] columns = new byte[cds.length][];
      for (int i = 0; i < cds.length; i++) {
        columns[i] = Bytes.add(cds[i].getName(),
            KeyValue.COLUMN_FAMILY_DELIM_ARRAY);
      }
      return columns;
    }

    /**
     * Creates and returns an HTable instance from a given table name.
     *
     * @param tableName
     *          name of table
     * @return HTable object
     * @throws IOException
     * @throws IOError
     */
    protected HTable getTable(final byte[] tableName) throws
        IOException {
      String table = new String(tableName);
      Map<String, HTable> tables = threadLocalTables.get();
      if (!tables.containsKey(table)) {
        tables.put(table, new HTable(conf, tableName));
      }
      return tables.get(table);
    }

    protected HTable getTable(final ByteBuffer tableName) throws IOException {
      return getTable(getBytes(tableName));
    }

    /**
     * Assigns a unique ID to the scanner and adds the mapping to an internal
     * hash-map.
     *
     * @param scanner
     * @return integer scanner id
     */
    protected int addScanner(ResultScanner scanner) {
      int id = nextScannerId.incrementAndGet();
      scannerMap.put(id, scanner);
      return id;
    }

    /**
     * Returns the scanner associated with the specified ID.
     *
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected ResultScanner getScanner(int id) {
      return scannerMap.get(id);
    }

    /**
     * Removes the scanner associated with the specified ID from the internal
     * id->scanner hash-map.
     *
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected ResultScanner removeScanner(int id) {
      return scannerMap.remove(id);
    }

    /**
     * Constructs an HBaseHandler object.
     * @throws IOException
     */
    HBaseHandler()
    throws IOException {
      this(HBaseConfiguration.create());
    }

    HBaseHandler(final Configuration c)
    throws IOException {
      this.conf = c;

      // be way more aggressive about time outs.
      conf.setInt("hbase.client.retries.number", 4);
      admin = new HBaseAdmin(conf);

      LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
      pool = new ThreadPoolExecutor(CORE_POOL_SIZE, CORE_POOL_SIZE,
          50, TimeUnit.MILLISECONDS,
          queue,
          new DaemonThreadFactory());

      int scannerTimeout =
          this.conf.getInt("hbase.thrift.scanner.cleaner.timeout", 6000000);
      ScannerCleaner cleaner =
          new ScannerCleaner("Scanner cleaner", scannerTimeout, STOPPABLE);
      cleaner.start();

      try {
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        ObjectName name = new ObjectName("hbase:" +
        "service=Thrift,name=" + Thread.currentThread().getName());

        mbs.registerMBean(this, name);

      } catch (MalformedObjectNameException e) {
        LOG.warn("JMX init failed, sorry :-(", e);
      } catch (NotCompliantMBeanException e) {
        LOG.warn("JMX init failed", e);
      } catch (InstanceAlreadyExistsException e) {
        LOG.warn("JMX init failed", e);
      } catch (MBeanRegistrationException e) {
        LOG.warn("JMX init failed", e);
      }
    }

    @Override
    public void enableTable(ByteBuffer tableName) throws IOError {
      try{
        admin.enableTable(getBytes(tableName));
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void disableTable(ByteBuffer tableName) throws IOError{
      try{
        admin.disableTable(getBytes(tableName));
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public boolean isTableEnabled(ByteBuffer tableName) throws IOError {
      try {
        return HTable.isTableEnabled(this.conf, getBytes(tableName));
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void compact(ByteBuffer tableNameOrRegionName) throws IOError {
      try{
        admin.compact(getBytes(tableNameOrRegionName));
      } catch (InterruptedException e) {
        throw new IOError(e.getMessage());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void majorCompact(ByteBuffer tableNameOrRegionName) throws IOError {
      try{
        admin.majorCompact(getBytes(tableNameOrRegionName));
      } catch (InterruptedException e) {
        throw new IOError(e.getMessage());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<ByteBuffer> getTableNames() throws IOError {
      try {
        HTableDescriptor[] tables = this.admin.listTables();
        ArrayList<ByteBuffer> list = new ArrayList<ByteBuffer>(tables.length);
        for (int i = 0; i < tables.length; i++) {
          list.add(ByteBuffer.wrap(tables[i].getName()));
        }
        return list;
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<TRegionInfo> getTableRegions(ByteBuffer tableName)
    throws IOError {
      try{
        List<HRegionInfo> hris = this.admin.getTableRegions(tableName.array());
        List<TRegionInfo> regions = new ArrayList<TRegionInfo>();

        if (hris != null) {
          for (HRegionInfo regionInfo : hris){
            TRegionInfo region = new TRegionInfo();
            region.startKey = ByteBuffer.wrap(regionInfo.getStartKey());
            region.endKey = ByteBuffer.wrap(regionInfo.getEndKey());
            region.id = regionInfo.getRegionId();
            region.name = ByteBuffer.wrap(regionInfo.getRegionName());
            region.version = regionInfo.getVersion();
            regions.add(region);
          }
        }
        return regions;
      } catch (IOException e){
        throw new IOError(e.getMessage());
      }
    }

    @Deprecated
    @Override
    public List<TCell> get(ByteBuffer tableName, ByteBuffer row, ByteBuffer column)
        throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return get(tableName, row, famAndQf[0], new byte[0]);
      }
      return get(tableName, row, famAndQf[0], famAndQf[1]);
    }

    protected List<TCell> get(ByteBuffer tableName,
                              ByteBuffer row,
                              byte[] family,
                              byte[] qualifier) throws IOError {
      try {
        HTable table = getTable(tableName);
        Get get = new Get(getBytes(row));
        if (qualifier == null || qualifier.length == 0) {
          get.addFamily(family);
        } else {
          get.addColumn(family, qualifier);
        }
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.raw());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Deprecated
    @Override
    public List<TCell> getVer(ByteBuffer tableName, ByteBuffer row,
        ByteBuffer column, int numVersions) throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return getVer(tableName, row, famAndQf[0],
            new byte[0], numVersions);
      }
      return getVer(tableName, row,
          famAndQf[0], famAndQf[1], numVersions);
    }

    public List<TCell> getVer(ByteBuffer tableName, ByteBuffer row,
                              byte[] family,
        byte[] qualifier, int numVersions) throws IOError {
      try {
        HTable table = getTable(tableName);
        Get get = new Get(getBytes(row));
        get.addColumn(family, qualifier);
        get.setMaxVersions(numVersions);
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.raw());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Deprecated
    @Override
    public List<TCell> getVerTs(ByteBuffer tableName,
                                   ByteBuffer row,
        ByteBuffer column,
        long timestamp,
        int numVersions) throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return getVerTs(tableName, row, famAndQf[0], new byte[0], timestamp,
            numVersions);
      }
      return getVerTs(tableName, row, famAndQf[0], famAndQf[1], timestamp,
          numVersions);
    }

    protected List<TCell> getVerTs(ByteBuffer tableName,
                                   ByteBuffer row, byte [] family,
        byte [] qualifier, long timestamp, int numVersions) throws IOError {
      try {
        HTable table = getTable(tableName);
        Get get = new Get(getBytes(row));
        get.addColumn(family, qualifier);
        get.setTimeRange(Long.MIN_VALUE, timestamp);
        get.setMaxVersions(numVersions);
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.raw());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<TRowResult> getRow(ByteBuffer tableName, ByteBuffer row)
        throws IOError {
      return getRowWithColumnsTs(tableName, row, null,
                                 HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public List<TRowResult> getRowWithColumns(ByteBuffer tableName,
                                              ByteBuffer row,
        List<ByteBuffer> columns) throws IOError {
      return getRowWithColumnsTs(tableName, row, columns,
                                 HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public List<TRowResult> getRowTs(ByteBuffer tableName, ByteBuffer row,
        long timestamp) throws IOError {
      return getRowWithColumnsTs(tableName, row, null,
                                 timestamp);
    }

    @Override
    public List<TRowResult> getRowWithColumnsTs(ByteBuffer tableName, ByteBuffer row,
        List<ByteBuffer> columns, long timestamp) throws IOError {
      try {
        HTable table = getTable(tableName);
        if (columns == null) {
          Get get = new Get(getBytes(row));
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          Result result = table.get(get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        Get get = new Get(getBytes(row));
        for(ByteBuffer column : columns) {
          byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
          if (famAndQf.length == 1) {
              get.addFamily(famAndQf[0]);
          } else {
              get.addColumn(famAndQf[0], famAndQf[1]);
          }
        }
        get.setTimeRange(Long.MIN_VALUE, timestamp);
        Result result = table.get(get);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<TRowResult> getRows(ByteBuffer tableName,
                                    List<ByteBuffer> rows)
        throws IOError {
      return getRowsWithColumnsTs(tableName, rows, null,
                                  HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public List<TRowResult> getRowsWithColumns(ByteBuffer tableName,
                                               List<ByteBuffer> rows,
        List<ByteBuffer> columns) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, columns,
                                  HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public List<TRowResult> getRowsTs(ByteBuffer tableName,
                                      List<ByteBuffer> rows,
        long timestamp) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, null,
                                  timestamp);
    }

    @Override
    public List<TRowResult> getRowsWithColumnsTs(ByteBuffer tableName,
                                                 List<ByteBuffer> rows,
        List<ByteBuffer> columns, long timestamp) throws IOError {
      try {
        List<Get> gets = new ArrayList<Get>(rows.size());
        HTable table = getTable(tableName);
        for (ByteBuffer row : rows) {
          Get get = new Get(getBytes(row));
          if (columns != null) {

            for(ByteBuffer column : columns) {
              byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
              if (famAndQf.length == 1) {
                get.addFamily(famAndQf[0]);
              } else {
                get.addColumn(famAndQf[0], famAndQf[1]);
              }
            }
            get.setTimeRange(Long.MIN_VALUE, timestamp);
          }
          gets.add(get);
        }
        Result[] result = table.get(gets);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void deleteAll(ByteBuffer tableName, ByteBuffer row, ByteBuffer column)
        throws IOError {
      deleteAllTs(tableName, row, column, HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public void deleteAllTs(ByteBuffer tableName,
                            ByteBuffer row,
                            ByteBuffer column,
        long timestamp) throws IOError {
      try {
        HTable table = getTable(tableName);
        Delete delete  = new Delete(getBytes(row));
        byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
        if (famAndQf.length == 1) {
          delete.deleteFamily(famAndQf[0], timestamp);
        } else {
          delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
        }
        table.delete(delete);

      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void deleteAllRow(ByteBuffer tableName, ByteBuffer row) throws IOError {
      deleteAllRowTs(tableName, row, HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public void deleteAllRowTs(ByteBuffer tableName, ByteBuffer row, long timestamp)
        throws IOError {
      try {
        HTable table = getTable(tableName);
        Delete delete  = new Delete(getBytes(row), timestamp, null);
        table.delete(delete);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void createTable(ByteBuffer in_tableName,
        List<ColumnDescriptor> columnFamilies) throws IOError,
        IllegalArgument, AlreadyExists {
      byte [] tableName = getBytes(in_tableName);
      try {
        if (admin.tableExists(tableName)) {
          throw new AlreadyExists("table name already in use");
        }
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (ColumnDescriptor col : columnFamilies) {
          HColumnDescriptor colDesc = ThriftUtilities.colDescFromThrift(col);
          desc.addFamily(colDesc);
        }
        admin.createTable(desc);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgument(e.getMessage());
      }
    }

    @Override
    public void deleteTable(ByteBuffer in_tableName) throws IOError {
      byte [] tableName = getBytes(in_tableName);
      if (LOG.isDebugEnabled()) {
        LOG.debug("deleteTable: table=" + Bytes.toString(tableName));
      }
      try {
        if (!admin.tableExists(tableName)) {
          throw new IOError("table does not exist");
        }
        admin.deleteTable(tableName);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void mutateRow(ByteBuffer tableName, ByteBuffer row,
        List<Mutation> mutations) throws IOError, IllegalArgument {
      mutateRowTs(tableName, row, mutations, HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public void mutateRowTs(ByteBuffer tableName, ByteBuffer row,
        List<Mutation> mutations, long timestamp) throws IOError, IllegalArgument {
      HTable table = null;
      try {
        table = getTable(tableName);
        Put put = new Put(getBytes(row), timestamp, null);

        Delete delete = new Delete(getBytes(row));

        // I apologize for all this mess :)
        for (Mutation m : mutations) {
          byte[][] famAndQf = KeyValue.parseColumn(getBytes(m.column));
          if (m.isDelete) {
            if (famAndQf.length == 1) {
              delete.deleteFamily(famAndQf[0], timestamp);
            } else {
              delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
            }
          } else {
            if(famAndQf.length == 1) {
              put.add(famAndQf[0], HConstants.EMPTY_BYTE_ARRAY,
                  m.value != null ? m.value.array()
                      : HConstants.EMPTY_BYTE_ARRAY);
            } else {
              put.add(famAndQf[0], famAndQf[1],
                  m.value != null ? m.value.array()
                      : HConstants.EMPTY_BYTE_ARRAY);
            }
          }
        }
        if (!delete.isEmpty())
          table.delete(delete);
        if (!put.isEmpty())
          table.put(put);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgument(e.getMessage());
      }
    }

    @Override
    public void mutateRows(ByteBuffer tableName, List<BatchMutation> rowBatches)
        throws IOError, IllegalArgument, TException {
      mutateRowsTs(tableName, rowBatches, HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public void mutateRowsTs(ByteBuffer tableName, List<BatchMutation> rowBatches, long timestamp)
        throws IOError, IllegalArgument, TException {
      List<Put> puts = new ArrayList<Put>();
      List<Delete> deletes = new ArrayList<Delete>();

      for (BatchMutation batch : rowBatches) {
        byte[] row = getBytes(batch.row);
        List<Mutation> mutations = batch.mutations;
        Delete delete = new Delete(row);
        Put put = new Put(row, timestamp, null);
        for (Mutation m : mutations) {
          byte[][] famAndQf = KeyValue.parseColumn(getBytes(m.column));
          if (m.isDelete) {
            // no qualifier, family only.
            if (famAndQf.length == 1) {
              delete.deleteFamily(famAndQf[0], timestamp);
            } else {
              delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
            }
          } else {
            if(famAndQf.length == 1) {
              put.add(famAndQf[0], HConstants.EMPTY_BYTE_ARRAY,
                  m.value != null ? m.value.array()
                      : HConstants.EMPTY_BYTE_ARRAY);
            } else {
              put.add(famAndQf[0], famAndQf[1],
                  m.value != null ? m.value.array()
                      : HConstants.EMPTY_BYTE_ARRAY);
            }
          }
        }
        if (!delete.isEmpty())
          deletes.add(delete);
        if (!put.isEmpty())
          puts.add(put);
      }

      HTable table = null;
      try {
        table = getTable(tableName);
        if (!puts.isEmpty())
          table.put(puts);
        for (Delete del : deletes) {
          table.delete(del);
        }
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgument(e.getMessage());
      }
    }

    @Deprecated
    @Override
    public long atomicIncrement(ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
        long amount) throws IOError, IllegalArgument, TException {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return atomicIncrement(tableName, row, famAndQf[0], new byte[0],
            amount);
      }
      return atomicIncrement(tableName, row, famAndQf[0], famAndQf[1], amount);
    }

    protected long atomicIncrement(ByteBuffer tableName, ByteBuffer row, byte [] family,
        byte [] qualifier, long amount)
    throws IOError, IllegalArgument, TException {
      HTable table;
      try {
        table = getTable(tableName);
        return table.incrementColumnValue(getBytes(row), family, qualifier, amount);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

     @Override
    public boolean queueIncrementColumnValues(List<Increment> increments) throws TException {
      int countersMapSize = countersMap.size();
      dynamicallySetCoreSize(countersMapSize);
      if (countersMapSize > failQueueSize) {
        failedIncrements.incrementAndGet();
        return false;
      }
      for (Increment i : increments) {
        totalIncrements.incrementAndGet();
        byte[][]famAndQf = KeyValue.parseColumn(i.getColumn());
        if (famAndQf.length != 2) {
          throw new TException("Bad column: " + Bytes.toString(i.getColumn()));
        }
        if (i.getRow().length == 0) {
          throw new TException("The row key is empty, can't increment");
        }

        // ugly!!!
        // Using KV as it has all the facilities to easily store/compare/hash
        // multiple byte[]. Less hacky would be to create our own structure
        KeyValue key = new KeyValue(i.getRow(), famAndQf[0],
            famAndQf[1], i.getTable());
        long currentAmount = i.getAmount();
        // Spin until able to insert the value back without collisions
        while (true) {
          Long value = countersMap.remove(key);
          if (value == null) {
            // There was nothing there, create a new value
            value = new Long(currentAmount);
          } else {
            value += currentAmount;
            successfulCoalescings.incrementAndGet();
          }
          // Try to put the value, only if there was none
          Long oldValue = countersMap.putIfAbsent(key, value);
          if (oldValue == null) {
            // We were able to put it in, we're done
            break;
          }
          // Someone else was able to put a value in, so let's remember our
          // current value (plus what we picked up) and retry to add it in
          currentAmount = value;
        }
      }
      // We limit the size of the queue simply because all we need is a
      // notification that something needs to be incremented. No need
      // for millions of callables that mean the same thing.
      if (pool.getQueue().size() <= 1000) {
         // queue it up
        Callable<Integer> callable = createIncCallable();
        pool.submit(callable);
      }

      return true;
    }

    /**
     * This method samples the incoming requests and, if selected, will
     * check if the corePoolSize should be changed.
     * @param countersMapSize
     */
    private void dynamicallySetCoreSize(int countersMapSize) {
      // Here we are using countersMapSize as a random number, meaning this
      // could be a Random object
      if (countersMapSize % 10 != 0) {
        return;
      }
      double currentRatio = (double)countersMapSize / (double)failQueueSize;
      int newValue = 1;
      if (currentRatio < 0.1) {
        // it's 1
      } else if (currentRatio < 0.3) {
        newValue = 2;
      } else if (currentRatio < 0.5) {
        newValue = 4;
      } else if (currentRatio < 0.7) {
        newValue = 8;
      } else if (currentRatio < 0.9) {
        newValue = 14;
      } else {
        newValue = 22;
      }
      if (pool.getCorePoolSize() != newValue) {
        pool.setCorePoolSize(newValue);
      }
    }

    @Override
    public List<TRowResult> parallelScan(ScanSpec spec,
                                         List<ByteBuffer> startRows,
                                         List<ByteBuffer> endRows)
        throws IOError, TException {
      return new ArrayList<TRowResult>();
    }

    @Override
    public ScanResult scan(ScanSpec spec, int nRows, boolean closeAfter)
        throws IOError, TException {

      // Build a Scan object, populate it, then execute scan and return.
      if (!spec.isSetTableName()) {
        throw new IOError("Spec.tableName needs to be set!");
      }
      if (spec.prefixScan && !spec.isSetStartRow()) {
        throw new IOError("If you set prefixScan you must also set startRow!");
      }
      if (spec.prefixScan && spec.isSetStopRow()) {
        throw new IOError("Prefix scan with stop row not allowed");
      }

      ResultScanner scanner = null;

      try {
        HTable table = getTable(spec.getTableName());

        // Set up scan!
        Scan scan = new Scan();
        if (spec.isSetStartRow()) {
          scan.setStartRow(spec.getStartRow());
        }
        if (spec.isSetStopRow()) {
          scan.setStopRow(spec.getStopRow());
        }
        if (spec.prefixScan) {
          Filter f = new PrefixFilter(spec.getStartRow());
          scan.setFilter(f);
        }
        if (spec.isSetColumns()) {
          for (ByteBuffer column : spec.getColumns()) {
            byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
            if (famAndQf.length == 1) {
              scan.addFamily(famAndQf[0]);
            } else {
              scan.addColumn(famAndQf[0], famAndQf[1]);
            }
          }
        }
        if (spec.isSetMaxVersions()) {
          scan.setMaxVersions(spec.getMaxVersions());
        }
        if (spec.isSetEndTime() || spec.isSetStartTime()) {
          // Process time filter here.
          long startTime = Long.MIN_VALUE;
          long endTime = Long.MAX_VALUE;
          if (spec.isSetStartTime()) {
            startTime = spec.getStartTime();
          }
          if (spec.isSetEndTime()) {
            endTime = spec.getEndTime();
          }
          scan.setTimeRange(startTime, endTime);
        }
        // boolean defaults to false, but we want to default to 'true',
        // so only set this if the client explicitly sets it.
        if (spec.isSetCacheBlocks()) {
          scan.setCacheBlocks(spec.cacheBlocks);
        }
        scan.setCaching(nRows+1);

        // Start to execute scan...
        scanner = table.getScanner(scan);
        // need to actually grab the next one!

        Result[] data = scanner.next(nRows);

        Result nextOne = null;

        // if we got the nRows worth, peek ahead and see if there is a next one.
        if (data.length == nRows) {
          nextOne = scanner.next();
        }

        int scannerId = 0;
        if (closeAfter || nextOne == null) {
          scanner.close();
        } else {
          ScannerAndNext sn = new ScannerAndNext(scanner,nextOne);
          scannerId = nextScannerId.incrementAndGet();
            newScannerMap.put(scannerId, sn);
        }

        // build our response.
        ScanResult scanResult = new ScanResult();
        scanResult.setResults(ThriftUtilities.rowResultFromHBase(data));
        if (closeAfter || nextOne == null) {
          scanResult.setHasMore(false);
        } else {
          scanResult.setHasMore(true);
          scanResult.setScannerId(scannerId);
        }

        return scanResult;
      } catch (IOException e) {
        if (scanner != null) {
          scanner.close();
        }
        throw new IOError(e.getMessage());
      }

    }

    @Override
    public ScanResult scanMore(int scannerId, int nRows, boolean closeAfter)
        throws IOError, TException {
      ScannerAndNext sn = newScannerMap.remove(scannerId);
      if (sn == null) {
        throw new IOError("No such scanner: " + scannerId);
      }

      try {
        Result [] data = null;
        // If we squirreled off a result in sn.next, need to produce it here.
        if (sn.next !=  null) {
          // We have a result from previous scan invocation.  Count it in.
          Result [] interrim = nRows <= 1? new Result [0]:
            sn.scanner.next(nRows - 1);
          // Now assemble in data the fetched results and what we had up in
          // sn.next, saved off from last call.
          data = new Result[interrim.length + 1];
          data[0] = sn.next;
          int index = 0;
          for (Result r: interrim) {
            // pre-increment because we have something in first position.
            data[++index] = r;
          }
        } else {
          data = sn.scanner.next(nRows);
        }

        Result next = null;
        if (data.length == nRows) {
          next = sn.scanner.next();
        }

        ScanResult scanResult = new ScanResult();
        scanResult.setResults(ThriftUtilities.rowResultFromHBase(data));
        if (closeAfter || next == null) {
          scanResult.setHasMore(false);
          sn.scanner.close();
        } else {
          // next != null
          scanResult.setHasMore(true);
          scanResult.setScannerId(scannerId);

          newScannerMap.put(scannerId,
              new ScannerAndNext(sn.scanner,
                  next));
        }
        return scanResult;
      } catch (IOException ex) {
        sn.scanner.close(); // scanner is dead now.

        throw new IOError(ex.getMessage());
      }
    }

    private Callable<Integer> createIncCallable() {
      return new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          int failures = 0;
          Set<KeyValue> keys = countersMap.keySet();
          for (KeyValue kv : keys) {
            Long counter = countersMap.remove(kv);
            if (counter == null) {
              continue;
            }
            try {
              HTable table = getTable(kv.getValue());
              if (failures > 2) {
                throw new IOException("Auto-Fail rest of ICVs");
              }
              table.incrementColumnValue(
                  kv.getRow(),
                  kv.getFamily(),
                  kv.getQualifier(),
                  counter);
            } catch (IOException e) {
              // log failure of increment
              failures++;
              LOG.error("FAILED_ICV: "
                  + Bytes.toString(kv.getValue()) + ", "
                  + Bytes.toStringBinary(kv.getRow()) + ", "
                  + Bytes.toStringBinary(kv.getFamily()) + ", "
                  + Bytes.toStringBinary(kv.getQualifier()) + ", "
                  + counter);
            }

          }
          return failures;
        }
      };
    }

    public void scannerClose(int id) throws IOError, IllegalArgument {
      LOG.debug("scannerClose: id=" + id);
      ResultScanner scanner = getScanner(id);
      if (scanner == null) {
        throw new IllegalArgument("scanner ID is invalid");
      }
      scanner.close();
      removeScanner(id);
    }

    @Override
    public List<TRowResult> scannerGetList(int id,int nbRows) throws IllegalArgument, IOError {
        LOG.debug("scannerGetList: id=" + id);
        ResultScanner scanner = getScanner(id);
        if (null == scanner) {
            throw new IllegalArgument("scanner ID is invalid");
        }

        Result [] results = null;
        try {
            results = scanner.next(nbRows);
            if (null == results) {
                return new ArrayList<TRowResult>();
            }
        } catch (IOException e) {
            throw new IOError(e.getMessage());
        }
        return ThriftUtilities.rowResultFromHBase(results);
    }

    @Override
    public List<TRowResult> parallelGet(ByteBuffer tableName,
                                        ByteBuffer column,
                                        List<ByteBuffer> rows) throws TException, IOError {
      try {
        HTable table = getTable(tableName);

        byte [][] famAndQual = column == null?
          null: KeyValue.parseColumn(getBytes(column));

        List<Row> gets = new ArrayList<Row>(rows.size());
        for (ByteBuffer row : rows) {
          Get g = new Get(getBytes(row));
          if (famAndQual == null) {
            // Return all
          } else if (famAndQual.length == 1) {
            g.addFamily(famAndQual[0]);
          } else {
            g.addColumn(famAndQual[0], famAndQual[1]);
          }
          gets.add(g);
        }

        Object[] results = table.batch(gets);

        Result[] results1 = new Result[results.length];
        int i=0;
        for( Object r : results) {
          results1[i++] = (Result)r;
        }

        return ThriftUtilities.rowResultFromHBase(results1);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      } catch (InterruptedException e) {
        throw new IOError(e.getMessage());
      }
    }


    @Override
    public List<TRowResult> scannerGet(int id) throws IllegalArgument, IOError {
        return scannerGetList(id,1);
    }

    public int scannerOpenWithScan(ByteBuffer tableName, TScan tScan) throws IOError {
        try {
          HTable table = getTable(tableName);
          Scan scan = new Scan();
          if (tScan.isSetStartRow()) {
              scan.setStartRow(tScan.getStartRow());
          }
          if (tScan.isSetStopRow()) {
              scan.setStopRow(tScan.getStopRow());
          }
          if (tScan.isSetTimestamp()) {
              scan.setTimeRange(Long.MIN_VALUE, tScan.getTimestamp());              
          }
          if (tScan.isSetCaching()) {
              scan.setCaching(tScan.getCaching());
          }
          if(tScan.isSetColumns() && tScan.getColumns().size() != 0) {
            for(ByteBuffer column : tScan.getColumns()) {
              byte [][] famQf = KeyValue.parseColumn(getBytes(column));
              if(famQf.length == 1) {
                scan.addFamily(famQf[0]);
              } else {
                scan.addColumn(famQf[0], famQf[1]);
              }
            }
          }
          if (tScan.isSetFilterString()) {
            ParseFilter parseFilter = new ParseFilter();
            scan.setFilter(parseFilter.parseFilterString(tScan.getFilterString()));
          }
          return addScanner(table.getScanner(scan));
        } catch (IOException e) {
          throw new IOError(e.getMessage());
        }
    }

    @Override
    public int scannerOpen(ByteBuffer tableName, ByteBuffer startRow,
            List<ByteBuffer> columns) throws IOError {
        try {
          HTable table = getTable(tableName);
          Scan scan = new Scan(getBytes(startRow));
          if(columns != null && columns.size() != 0) {
            for(ByteBuffer column : columns) {
              byte [][] famQf = KeyValue.parseColumn(getBytes(column));
              if(famQf.length == 1) {
                scan.addFamily(famQf[0]);
              } else {
                scan.addColumn(famQf[0], famQf[1]);
              }
            }
          }
          return addScanner(table.getScanner(scan));
        } catch (IOException e) {
          throw new IOError(e.getMessage());
        }
    }

    @Override
    public int scannerOpenWithStop(ByteBuffer tableName, ByteBuffer startRow,
        ByteBuffer stopRow, List<ByteBuffer> columns) throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow), getBytes(stopRow));
        if(columns != null && columns.size() != 0) {
          for(ByteBuffer column : columns) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        return addScanner(table.getScanner(scan));
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public int scannerOpenWithPrefix(ByteBuffer tableName,
                                     ByteBuffer startAndPrefix,
                                     List<ByteBuffer> columns)
        throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startAndPrefix));
        Filter f = new WhileMatchFilter(
            new PrefixFilter(getBytes(startAndPrefix)));
        scan.setFilter(f);
        if(columns != null && columns.size() != 0) {
          for(ByteBuffer column : columns) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        return addScanner(table.getScanner(scan));
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public int scannerOpenTs(ByteBuffer tableName, ByteBuffer startRow,
        List<ByteBuffer> columns, long timestamp) throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow));
        scan.setTimeRange(Long.MIN_VALUE, timestamp);
        if(columns != null && columns.size() != 0) {
          for(ByteBuffer column : columns) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        return addScanner(table.getScanner(scan));
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public int scannerOpenWithStopTs(ByteBuffer tableName, ByteBuffer startRow,
        ByteBuffer stopRow, List<ByteBuffer> columns, long timestamp)
        throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow), getBytes(stopRow));
        scan.setTimeRange(Long.MIN_VALUE, timestamp);
        if(columns != null && columns.size() != 0) {
          for(ByteBuffer column : columns) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        scan.setTimeRange(Long.MIN_VALUE, timestamp);
        return addScanner(table.getScanner(scan));
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public Map<ByteBuffer, ColumnDescriptor> getColumnDescriptors(
        ByteBuffer tableName) throws IOError, TException {
      try {
        TreeMap<ByteBuffer, ColumnDescriptor> columns =
          new TreeMap<ByteBuffer, ColumnDescriptor>();

        HTable table = getTable(tableName);
        HTableDescriptor desc = table.getTableDescriptor();

        for (HColumnDescriptor e : desc.getFamilies()) {
          ColumnDescriptor col = ThriftUtilities.colDescFromHbase(e);
          columns.put(col.name, col);
        }
        return columns;
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    /**
     * Class that's used to clean the scanners that aren't closed after the
     * defined period of time. Only works for the new scanners
     */
    class ScannerCleaner extends Chore {

      final int timeout;

      public ScannerCleaner(String name, final int p,
                            final Stoppable stopper) {
        super(name, p, stopper);
        this.timeout = p;
      }

      @Override
      protected void chore() {

        // Iterate over all the current scanners
        for (java.util.Map.Entry<Integer, ScannerAndNext> scanAndNext :
            newScannerMap.entrySet()) {
          ScannerAndNext scan = scanAndNext.getValue();

          // if over the timeout, clean
          if ((System.currentTimeMillis() - scan.lastTimeUsed) > this.timeout) {
            Integer key = scanAndNext.getKey();
            Result next = scan.next;
            LOG.info("ATTENTION This scanner has been there for a long time," +
                " closing: " + key + (scan.next == null ? "" :
                ", and here's the next row: " + next.toString()) );
            newScannerMap.remove(key);
            // This is normally expired
            scan.scanner.close();
          }
        }
      }
    }
  }

  //
  // Main program and support routines
  //

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Thrift", null, options,
            "To start the Thrift server run 'bin/hbase-daemon.sh start thrift'\n" +
            "To shutdown the thrift server run 'bin/hbase-daemon.sh stop thrift' or" +
            " send a kill signal to the thrift server pid",
            true);
      System.exit(exitCode);
  }

  private static final String DEFAULT_LISTEN_PORT = "9090";

  /*
   * Start up the Thrift server.
   * @param args
   */
  static private void doMain(final String[] args) throws Exception {
    Log LOG = LogFactory.getLog("ThriftServer");

    Options options = new Options();
    options.addOption("b", "bind", true, "Address to bind the Thrift server to. Not supported by the Nonblocking and HsHa server [default: 0.0.0.0]");
    options.addOption("p", "port", true, "Port to bind to [default: 9090]");
    options.addOption("f", "framed", false, "Use framed transport");
    options.addOption("c", "compact", false, "Use the compact protocol");
    options.addOption("h", "help", false, "Print help information");

    OptionGroup servers = new OptionGroup();
    servers.addOption(new Option("nonblocking", false, "Use the TNonblockingServer. This implies the framed transport."));
    servers.addOption(new Option("hsha", false, "Use the THsHaServer. This implies the framed transport."));
    servers.addOption(new Option("threadpool", false, "Use the TThreadPoolServer. This is the default."));
    options.addOptionGroup(servers);

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    /**
     * This is so complicated to please both bin/hbase and bin/hbase-daemon.
     * hbase-daemon provides "start" and "stop" arguments
     * hbase should print the help if no argument is provided
     */
    List<String> commandLine = Arrays.asList(args);
    boolean stop = commandLine.contains("stop");
    boolean start = commandLine.contains("start");
    if (cmd.hasOption("help") || !start || stop) {
      printUsageAndExit(options, 1);
    }

    // Get port to bind to
    int listenPort = 0;
    try {
      listenPort = Integer.parseInt(cmd.getOptionValue("port", DEFAULT_LISTEN_PORT));
    } catch (NumberFormatException e) {
      LOG.error("Could not parse the value provided for the port option", e);
      printUsageAndExit(options, -1);
    }

    // Construct correct ProtocolFactory
    TProtocolFactory protocolFactory;
    if (cmd.hasOption("compact")) {
      LOG.debug("Using compact protocol");
      protocolFactory = new TCompactProtocol.Factory();
    } else {
      LOG.debug("Using binary protocol");
      protocolFactory = new TBinaryProtocol.Factory();
    }

    HBaseHandler handler = new HBaseHandler();
    Hbase.Processor processor = new Hbase.Processor(handler);

    TServer server;
    if (cmd.hasOption("nonblocking") || cmd.hasOption("hsha")) {
      if (cmd.hasOption("bind")) {
        LOG.error("The Nonblocking and HsHa servers don't support IP address binding at the moment." +
                " See https://issues.apache.org/jira/browse/HBASE-2155 for details.");
        printUsageAndExit(options, -1);
      }

      TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(listenPort);
      TFramedTransport.Factory transportFactory = new TFramedTransport.Factory();

     if (cmd.hasOption("nonblocking")) {
        TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.transportFactory(transportFactory);
        serverArgs.protocolFactory(protocolFactory);

        LOG.info("starting HBase Nonblocking Thrift server on " + Integer.toString(listenPort));
        server = new TNonblockingServer(serverArgs);
      } else {
        THsHaServer.Args serverArgs = new THsHaServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.transportFactory(transportFactory);
        serverArgs.protocolFactory(protocolFactory);

        LOG.info("starting HBase HsHA Thrift server on " + Integer.toString(listenPort));
        server = new THsHaServer(serverArgs);
      }
    } else {
      // Get IP address to bind to
      InetAddress listenAddress = null;
      if (cmd.hasOption("bind")) {
        try {
          listenAddress = InetAddress.getByName(cmd.getOptionValue("bind"));
        } catch (UnknownHostException e) {
          LOG.error("Could not bind to provided ip address", e);
          printUsageAndExit(options, -1);
        }
      } else {
        listenAddress = InetAddress.getByName("0.0.0.0");
      }
      TServerTransport serverTransport = new TServerSocket(new InetSocketAddress(listenAddress, listenPort));

      // Construct correct TransportFactory
      TTransportFactory transportFactory;
      if (cmd.hasOption("framed")) {
        transportFactory = new TFramedTransport.Factory();
        LOG.debug("Using framed transport");
      } else {
        transportFactory = new TTransportFactory();
      }

      TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
      serverArgs.processor(processor);
      serverArgs.protocolFactory(protocolFactory);
      serverArgs.transportFactory(transportFactory);
      LOG.info("starting HBase ThreadPool Thrift server on " + listenAddress + ":" + Integer.toString(listenPort));
      server = new TThreadPoolServer(serverArgs);
    }

    // login the server principal (if using secure Hadoop)   
    Configuration conf = handler.conf;
    if (User.isSecurityEnabled() && User.isHBaseSecurityEnabled(conf)) {
      String machineName = Strings.domainNamePointerToHostName(
        DNS.getDefaultHost(conf.get("hbase.thrift.dns.interface", "default"),
          conf.get("hbase.thrift.dns.nameserver", "default")));
      User.login(conf, "hbase.thrift.keytab.file", "hbase.thrift.kerberos.principal",
        machineName);
    }

    server.serve();
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String [] args) throws Exception {
	VersionInfo.logVersion();
    doMain(args);
  }
}
