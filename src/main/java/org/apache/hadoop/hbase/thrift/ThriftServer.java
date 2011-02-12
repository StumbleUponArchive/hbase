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
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
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
import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.Increment;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRegionInfo;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
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
      return nextScannerId;
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
      return failedIncrements;
    }

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

    // nextScannerId and scannerMap are used to manage scanner state
    protected int nextScannerId = 0;
    protected HashMap<Integer, ResultScanner> scannerMap = null;

    private static ThreadLocal<Map<String, HTable>> threadLocalTables = new ThreadLocal<Map<String, HTable>>() {
      @Override
      protected Map<String, HTable> initialValue() {
        return new TreeMap<String, HTable>();
      }
    };
    private final ThreadPoolExecutor pool;

    private int failQueueSize = 10000;
    private static final int CORE_POOL_SIZE = 35;

    private volatile long failedIncrements = 0;


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
    protected synchronized int addScanner(ResultScanner scanner) {
      int id = nextScannerId++;
      scannerMap.put(id, scanner);
      return id;
    }

    /**
     * Returns the scanner associated with the specified ID.
     *
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized ResultScanner getScanner(int id) {
      return scannerMap.get(id);
    }

    /**
     * Removes the scanner associated with the specified ID from the internal
     * id->scanner hash-map.
     *
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized ResultScanner removeScanner(int id) {
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
      scannerMap = new HashMap<Integer, ResultScanner>();

      LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
      pool = new ThreadPoolExecutor(CORE_POOL_SIZE, CORE_POOL_SIZE,
          60, TimeUnit.SECONDS,
          queue,
          new DaemonThreadFactory());

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
        HTable table = getTable(tableName);
        Map<HRegionInfo, HServerAddress> regionsInfo = table.getRegionsInfo();
        List<TRegionInfo> regions = new ArrayList<TRegionInfo>();

        for (HRegionInfo regionInfo : regionsInfo.keySet()){
          TRegionInfo region = new TRegionInfo();
          region.startKey = ByteBuffer.wrap(regionInfo.getStartKey());
          region.endKey = ByteBuffer.wrap(regionInfo.getEndKey());
          region.id = regionInfo.getRegionId();
          region.name = ByteBuffer.wrap(regionInfo.getRegionName());
          region.version = regionInfo.getVersion();
          regions.add(region);
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
        return ThriftUtilities.cellFromHBase(result.sorted());
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
        return ThriftUtilities.cellFromHBase(result.sorted());
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
        return ThriftUtilities.cellFromHBase(result.sorted());
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
    public List<TRowResult> getRows(ByteBuffer tableName, List<ByteBuffer> rows)
        throws IOError {
      return getRowsWithColumnsTs(tableName, rows, null,
                                  HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public List<TRowResult> getRowsWithColumns(ByteBuffer tableName, List<ByteBuffer> rows,
        List<ByteBuffer> columns) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, columns,
                                  HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public List<TRowResult> getRowsTs(ByteBuffer tableName, List<ByteBuffer> rows,
        long timestamp) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, null,
                                  timestamp);
    }

    @Override
    public List<TRowResult> getRowsWithColumnsTs(ByteBuffer tableName, List<ByteBuffer> rows,
        List<ByteBuffer> columns, long timestamp) throws IOError {
      try {
        List<Get> gets = new ArrayList<Get>(rows.size());
        HTable table = getTable(tableName);
        for (ByteBuffer row : rows) {
          Get get = new Get(getBytes(row));
          if (columns != null) {
            for (ByteBuffer column : columns) {
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
              put.add(famAndQf[0], new byte[0], getBytes(m.value));
            } else {
              put.add(famAndQf[0], famAndQf[1], getBytes(m.value));
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
              put.add(famAndQf[0], new byte[0], getBytes(m.value));
            } else {
              put.add(famAndQf[0], famAndQf[1], getBytes(m.value));
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

    @Override
    public boolean queueIncrementColumnValues(List<Increment> increments) throws TException {
      if (pool.getQueue().size() > failQueueSize) {
        ++ failedIncrements;
        return false;
      }

      // queue it up
      Callable<Integer> callable = createIncCallable(increments);
      pool.submit(callable);

      return true;
    }

    private Callable<Integer> createIncCallable(final List<Increment> incrs) {
      return new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          int failures = 0;
          for (Increment incr : incrs) {
            try {
              HTable table = getTable(incr.getTable());
              byte [][]famAndQf = KeyValue.parseColumn(incr.getColumn());
              if (famAndQf.length != 2)
                throw new IOException("Bad column: " + Bytes.toString(incr.getColumn()));
              if (failures > 2) {
                throw new IOException("Auto-Fail rest of ICVs");
              }
              table.incrementColumnValue(
                  incr.getRow(),
                  famAndQf[0],
                  famAndQf[1],
                  incr.getAmount());
            } catch (IOException e) {
              // log failure of increment
              failures++;
              LOG.error("FAILED_ICV: "
                  + Bytes.toString(incr.getTable()) + ", "
                  + Bytes.toStringBinary(incr.getRow()) + ", "
                  + Bytes.toStringBinary(incr.getColumn()) + ", "
                  + incr.getAmount());
            }
          }
          return failures;
        }
      };
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
    public List<TRowResult> parallelGet(ByteBuffer tableName,
                                        ByteBuffer column,
                                        List<ByteBuffer> rows) throws TException, IOError {
      try {
        HTable table = getTable(tableName);

        byte [][] famAndQual = KeyValue.parseColumn(getBytes(column));

        List<Row> gets = new ArrayList<Row>(rows.size());
        for (ByteBuffer row : rows) {
          Get g = new Get(getBytes(row));
          if (famAndQual.length == 1) {
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
    public List<TRowResult> scannerGet(int id) throws IllegalArgument, IOError {
        return scannerGetList(id,1);
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
        scan.setTimeRange(timestamp, Long.MAX_VALUE);
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
        LOG.info("starting HBase Nonblocking Thrift server on " + Integer.toString(listenPort));
        server = new TNonblockingServer(processor, serverTransport, transportFactory, protocolFactory);
      } else {
        LOG.info("starting HBase HsHA Thrift server on " + Integer.toString(listenPort));
        server = new THsHaServer(processor, serverTransport, transportFactory, protocolFactory);
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

      LOG.info("starting HBase ThreadPool Thrift server on " + listenAddress + ":" + Integer.toString(listenPort));
      server = new TThreadPoolServer(processor, serverTransport, transportFactory, protocolFactory);
    }

    server.serve();
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String [] args) throws Exception {
    doMain(args);
  }
}
