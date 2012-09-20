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

import static org.apache.hadoop.hbase.util.Bytes.getBytes;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.thrift.CallQueue.Call;
import org.apache.hadoop.hbase.thrift.generated.*;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.net.DNS;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * ThriftServerRunner - this class starts up a Thrift server which implements
 * the Hbase API specified in the Hbase.thrift IDL file.
 */
public class ThriftServerRunner implements Runnable {

  private static final Log LOG = LogFactory.getLog(ThriftServerRunner.class);

  static final String SERVER_TYPE_CONF_KEY =
      "hbase.regionserver.thrift.server.type";

  static final String BIND_CONF_KEY = "hbase.regionserver.thrift.ipaddress";
  static final String COMPACT_CONF_KEY = "hbase.regionserver.thrift.compact";
  static final String FRAMED_CONF_KEY = "hbase.regionserver.thrift.framed";
  static final String PORT_CONF_KEY = "hbase.regionserver.thrift.port";
  static final String COALESCE_INC_KEY = "hbase.regionserver.thrift.coalesceIncrement";

  private static final String DEFAULT_BIND_ADDR = "0.0.0.0";
  public static final int DEFAULT_LISTEN_PORT = 9090;
  private final int listenPort;

  private Configuration conf;
  volatile TServer tserver;
  private final Hbase.Iface handler;
  private final ThriftMetrics metrics;

  /** An enum of server implementation selections */
  enum ImplType {
    HS_HA("hsha", true, THsHaServer.class, false),
    NONBLOCKING("nonblocking", true, TNonblockingServer.class, false),
    THREAD_POOL("threadpool", false, TBoundedThreadPoolServer.class, true),
    THREADED_SELECTOR(
        "threadedselector", true, TThreadedSelectorServer.class, false);

    public static final ImplType DEFAULT = THREAD_POOL;

    final String option;
    final boolean isAlwaysFramed;
    final Class<? extends TServer> serverClass;
    final boolean canSpecifyBindIP;

    ImplType(String option, boolean isAlwaysFramed,
        Class<? extends TServer> serverClass, boolean canSpecifyBindIP) {
      this.option = option;
      this.isAlwaysFramed = isAlwaysFramed;
      this.serverClass = serverClass;
      this.canSpecifyBindIP = canSpecifyBindIP;
    }

    /**
     * @return <code>-option</code> so we can get the list of options from
     *         {@link #values()}
     */
    @Override
    public String toString() {
      return "-" + option;
    }

    String getDescription() {
      StringBuilder sb = new StringBuilder("Use the " +
          serverClass.getSimpleName());
      if (isAlwaysFramed) {
        sb.append(" This implies the framed transport.");
      }
      if (this == DEFAULT) {
        sb.append("This is the default.");
      }
      return sb.toString();
    }

    static OptionGroup createOptionGroup() {
      OptionGroup group = new OptionGroup();
      for (ImplType t : values()) {
        group.addOption(new Option(t.option, t.getDescription()));
      }
      return group;
    }

    static ImplType getServerImpl(Configuration conf) {
      String confType = conf.get(SERVER_TYPE_CONF_KEY, THREAD_POOL.option);
      for (ImplType t : values()) {
        if (confType.equals(t.option)) {
          return t;
        }
      }
      throw new AssertionError("Unknown server ImplType.option:" + confType);
    }

    static void setServerImpl(CommandLine cmd, Configuration conf) {
      ImplType chosenType = null;
      int numChosen = 0;
      for (ImplType t : values()) {
        if (cmd.hasOption(t.option)) {
          chosenType = t;
          ++numChosen;
        }
      }
      if (numChosen < 1) {
        LOG.info("Using default thrift server type");
        chosenType = DEFAULT;
      } else if (numChosen > 1) {
        throw new AssertionError("Exactly one option out of " +
          Arrays.toString(values()) + " has to be specified");
      }
      LOG.info("Using thrift server type " + chosenType.option);
      conf.set(SERVER_TYPE_CONF_KEY, chosenType.option);
    }

    public String simpleClassName() {
      return serverClass.getSimpleName();
    }

    public static List<String> serversThatCannotSpecifyBindIP() {
      List<String> l = new ArrayList<String>();
      for (ImplType t : values()) {
        if (!t.canSpecifyBindIP) {
          l.add(t.simpleClassName());
        }
      }
      return l;
    }

  }

  public ThriftServerRunner(Configuration conf) throws IOException {
    this(conf, new ThriftServerRunner.HBaseHandler(conf));
  }

  public ThriftServerRunner(Configuration conf, HBaseHandler handler) {
    this.conf = HBaseConfiguration.create(conf);
    this.listenPort = conf.getInt(PORT_CONF_KEY, DEFAULT_LISTEN_PORT);
    this.metrics = new ThriftMetrics(listenPort, conf, Hbase.Iface.class);
    handler.initMetrics(metrics);
    this.handler = HbaseHandlerMetricsProxy.newInstance(handler, metrics, conf);
  }

  /*
   * Runs the Thrift server
   */
  @Override
  public void run() {
    try {
      setupServer();
      tserver.serve();
    } catch (Exception e) {
      LOG.fatal("Cannot run ThriftServer", e);
      // Crash the process if the ThriftServer is not running
      System.exit(-1);
    }
  }

  public void shutdown() {
    if (tserver != null) {
      tserver.stop();
      tserver = null;
    }
  }

  /**
   * Setting up the thrift TServer
   */
  private void setupServer() throws Exception {
    // Construct correct ProtocolFactory
    TProtocolFactory protocolFactory;
    if (conf.getBoolean(COMPACT_CONF_KEY, false)) {
      LOG.debug("Using compact protocol");
      protocolFactory = new TCompactProtocol.Factory();
    } else {
      LOG.debug("Using binary protocol");
      protocolFactory = new TBinaryProtocol.Factory();
    }

    Hbase.Processor<Hbase.Iface> processor =
        new Hbase.Processor<Hbase.Iface>(handler);
    ImplType implType = ImplType.getServerImpl(conf);

    // Construct correct TransportFactory
    TTransportFactory transportFactory;
    if (conf.getBoolean(FRAMED_CONF_KEY, false) || implType.isAlwaysFramed) {
      transportFactory = new TFramedTransport.Factory();
      LOG.debug("Using framed transport");
    } else {
      transportFactory = new TTransportFactory();
    }

    if (conf.get(BIND_CONF_KEY) != null && !implType.canSpecifyBindIP) {
      LOG.error("Server types " + Joiner.on(", ").join(
          ImplType.serversThatCannotSpecifyBindIP()) + " don't support IP " +
          "address binding at the moment. See " +
          "https://issues.apache.org/jira/browse/HBASE-2155 for details.");
      throw new RuntimeException(
          "-" + BIND_CONF_KEY + " not supported with " + implType);
    }

    if (implType == ImplType.HS_HA || implType == ImplType.NONBLOCKING ||
        implType == ImplType.THREADED_SELECTOR) {

      TNonblockingServerTransport serverTransport =
          new TNonblockingServerSocket(listenPort);

      if (implType == ImplType.NONBLOCKING) {
        TNonblockingServer.Args serverArgs =
            new TNonblockingServer.Args(serverTransport);
        serverArgs.processor(processor)
                  .transportFactory(transportFactory)
                  .protocolFactory(protocolFactory);
        tserver = new TNonblockingServer(serverArgs);
      } else if (implType == ImplType.HS_HA) {
        THsHaServer.Args serverArgs = new THsHaServer.Args(serverTransport);
        CallQueue callQueue =
            new CallQueue(new LinkedBlockingQueue<Call>(), metrics);
        ExecutorService executorService = createExecutor(
            callQueue, serverArgs.getWorkerThreads());
        serverArgs.executorService(executorService)
                  .processor(processor)
                  .transportFactory(transportFactory)
                  .protocolFactory(protocolFactory);
        tserver = new THsHaServer(serverArgs);
      } else { // THREADED_SELECTOR
        TThreadedSelectorServer.Args serverArgs =
            new HThreadedSelectorServerArgs(serverTransport, conf);
        CallQueue callQueue =
            new CallQueue(new LinkedBlockingQueue<Call>(), metrics);
        ExecutorService executorService = createExecutor(
            callQueue, serverArgs.getWorkerThreads());
        serverArgs.executorService(executorService)
                  .processor(processor)
                  .transportFactory(transportFactory)
                  .protocolFactory(protocolFactory);
        tserver = new TThreadedSelectorServer(serverArgs);
      }
      LOG.info("starting HBase " + implType.simpleClassName() +
          " server on " + Integer.toString(listenPort));
    } else if (implType == ImplType.THREAD_POOL) {
      // Thread pool server. Get the IP address to bind to.
      InetAddress listenAddress = getBindAddress(conf);

      TServerTransport serverTransport = new TServerSocket(
          new InetSocketAddress(listenAddress, listenPort));

      TBoundedThreadPoolServer.Args serverArgs =
          new TBoundedThreadPoolServer.Args(serverTransport, conf);
      serverArgs.processor(processor)
                .transportFactory(transportFactory)
                .protocolFactory(protocolFactory);
      LOG.info("starting " + ImplType.THREAD_POOL.simpleClassName() + " on "
          + listenAddress + ":" + Integer.toString(listenPort)
          + "; " + serverArgs);
      TBoundedThreadPoolServer tserver =
          new TBoundedThreadPoolServer(serverArgs, metrics);
      this.tserver = tserver;
    } else {
      throw new AssertionError("Unsupported Thrift server implementation: " +
          implType.simpleClassName());
    }

    // A sanity check that we instantiated the right type of server.
    if (tserver.getClass() != implType.serverClass) {
      throw new AssertionError("Expected to create Thrift server class " +
          implType.serverClass.getName() + " but got " +
          tserver.getClass().getName());
    }

    // login the server principal (if using secure Hadoop)
    if (User.isSecurityEnabled() && User.isHBaseSecurityEnabled(conf)) {
      String machineName = Strings.domainNamePointerToHostName(
        DNS.getDefaultHost(conf.get("hbase.thrift.dns.interface", "default"),
          conf.get("hbase.thrift.dns.nameserver", "default")));
      User.login(conf, "hbase.thrift.keytab.file",
          "hbase.thrift.kerberos.principal", machineName);
    }

    registerFilters(conf);
  }

  ExecutorService createExecutor(BlockingQueue<Runnable> callQueue,
                                 int workerThreads) {
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setDaemon(true);
    tfb.setNameFormat("thrift-worker-%d");
    return new ThreadPoolExecutor(workerThreads, workerThreads,
            Long.MAX_VALUE, TimeUnit.SECONDS, callQueue, tfb.build());
  }

  private InetAddress getBindAddress(Configuration conf)
      throws UnknownHostException {
    String bindAddressStr = conf.get(BIND_CONF_KEY, DEFAULT_BIND_ADDR);
    return InetAddress.getByName(bindAddressStr);
  }

  /**
   * The HBaseHandler is a glue object that connects Thrift RPC calls to the
   * HBase client API primarily defined in the HBaseAdmin and HTable objects.
   */
  public static class HBaseHandler implements Hbase.Iface {
    protected Configuration conf;
    protected HBaseAdmin admin = null;
    protected final Log LOG = LogFactory.getLog(this.getClass().getName());

    // nextScannerId and scannerMap are used to manage scanner state
    protected AtomicInteger nextScannerId = new AtomicInteger();
    protected HashMap<Integer, ResultScanner> scannerMap = null;
    private ThriftMetrics metrics = null;

    private static ThreadLocal<Map<String, HTable>> threadLocalTables =
        new ThreadLocal<Map<String, HTable>>() {
      @Override
      protected Map<String, HTable> initialValue() {
        return new TreeMap<String, HTable>();
      }
    };

    IncrementCoalescer coalescer = null;

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
    public HTable getTable(final byte[] tableName) throws
        IOException {
      String table = new String(tableName);
      Map<String, HTable> tables = threadLocalTables.get();
      if (!tables.containsKey(table)) {
        tables.put(table, new HTable(conf, tableName));
      }
      return tables.get(table);
    }

    public HTable getTable(final ByteBuffer tableName) throws IOException {
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
      int id = nextScannerId.getAndIncrement();
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
    protected HBaseHandler()
    throws IOException {
      this(HBaseConfiguration.create());
    }

    protected HBaseHandler(final Configuration c) throws IOException {
      this.conf = c;
      admin = new HBaseAdmin(conf);
      scannerMap = new HashMap<Integer, ResultScanner>();
      this.coalescer = new IncrementCoalescer(this);

      int scannerTimeout =
          this.conf.getInt("hbase.thrift.scanner.cleaner.timeout", 6000000);
      ScannerCleaner cleaner =
          new ScannerCleaner("Scanner cleaner", scannerTimeout, STOPPABLE);
      cleaner.start();
    }

    @Override
    public void enableTable(ByteBuffer tableName) throws IOError {
      try{
        admin.enableTable(getBytes(tableName));
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void disableTable(ByteBuffer tableName) throws IOError{
      try{
        admin.disableTable(getBytes(tableName));
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public boolean isTableEnabled(ByteBuffer tableName) throws IOError {
      try {
        return HTable.isTableEnabled(this.conf, getBytes(tableName));
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
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
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void majorCompact(ByteBuffer tableNameOrRegionName) throws IOError {
      try{
        admin.majorCompact(getBytes(tableNameOrRegionName));
      } catch (InterruptedException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
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
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<TRegionInfo> getTableRegions(ByteBuffer tableName)
    throws IOError {
      try {
        HTable table = getTable(tableName);
        Map<HRegionInfo, ServerName> regionLocations =
            table.getRegionLocations();
        List<TRegionInfo> results = new ArrayList<TRegionInfo>();
        for (Map.Entry<HRegionInfo, ServerName> entry :
            regionLocations.entrySet()) {
          HRegionInfo info = entry.getKey();
          ServerName serverName = entry.getValue();
          TRegionInfo region = new TRegionInfo();
          region.serverName = ByteBuffer.wrap(
              Bytes.toBytes(serverName.getHostname()));
          region.port = serverName.getPort();
          region.startKey = ByteBuffer.wrap(info.getStartKey());
          region.endKey = ByteBuffer.wrap(info.getEndKey());
          region.id = info.getRegionId();
          region.name = ByteBuffer.wrap(info.getRegionName());
          region.version = info.getVersion();
          results.add(region);
        }
        return results;
      } catch (TableNotFoundException e) {
        // Return empty list for non-existing table
        return Collections.emptyList();
      } catch (IOException e){
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    /**
     * Convert ByteBuffer to byte array. Note that this cannot be replaced by
     * Bytes.toBytes().
     */
    public static byte[] toBytes(ByteBuffer bb) {
      byte[] result = new byte[bb.remaining()];
      // Make a duplicate so the position doesn't change
      ByteBuffer dup = bb.duplicate();
      dup.get(result, 0, result.length);
      return result;
    }

    @Deprecated
    @Override
    public List<TCell> get(
        ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return get(tableName, row, famAndQf[0], new byte[0], attributes);
      }
      return get(tableName, row, famAndQf[0], famAndQf[1], attributes);
    }

    protected List<TCell> get(ByteBuffer tableName,
                              ByteBuffer row,
                              byte[] family,
                              byte[] qualifier,
                              Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      try {
        HTable table = getTable(tableName);
        Get get = new Get(getBytes(row));
        addAttributes(get, attributes);
        if (qualifier == null || qualifier.length == 0) {
          get.addFamily(family);
        } else {
          get.addColumn(family, qualifier);
        }
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.raw());
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Deprecated
    @Override
    public List<TCell> getVer(ByteBuffer tableName, ByteBuffer row,
        ByteBuffer column, int numVersions,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return getVer(tableName, row, famAndQf[0],
            new byte[0], numVersions, attributes);
      }
      return getVer(tableName, row,
          famAndQf[0], famAndQf[1], numVersions, attributes);
    }

    public List<TCell> getVer(ByteBuffer tableName, ByteBuffer row,
                              byte[] family,
        byte[] qualifier, int numVersions,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      try {
        HTable table = getTable(tableName);
        Get get = new Get(getBytes(row));
        addAttributes(get, attributes);
        get.addColumn(family, qualifier);
        get.setMaxVersions(numVersions);
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.raw());
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Deprecated
    @Override
    public List<TCell> getVerTs(ByteBuffer tableName,
                                   ByteBuffer row,
        ByteBuffer column,
        long timestamp,
        int numVersions,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return getVerTs(tableName, row, famAndQf[0], new byte[0], timestamp,
            numVersions, attributes);
      }
      return getVerTs(tableName, row, famAndQf[0], famAndQf[1], timestamp,
          numVersions, attributes);
    }

    protected List<TCell> getVerTs(ByteBuffer tableName,
                                   ByteBuffer row, byte [] family,
        byte [] qualifier, long timestamp, int numVersions,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      try {
        HTable table = getTable(tableName);
        Get get = new Get(getBytes(row));
        addAttributes(get, attributes);
        get.addColumn(family, qualifier);
        get.setTimeRange(Long.MIN_VALUE, timestamp);
        get.setMaxVersions(numVersions);
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.raw());
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<TRowResult> getRow(ByteBuffer tableName, ByteBuffer row,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      return getRowWithColumnsTs(tableName, row, null,
                                 HConstants.LATEST_TIMESTAMP,
                                 attributes);
    }

    @Override
    public List<TRowResult> getRowWithColumns(ByteBuffer tableName,
                                              ByteBuffer row,
        List<ByteBuffer> columns,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      return getRowWithColumnsTs(tableName, row, columns,
                                 HConstants.LATEST_TIMESTAMP,
                                 attributes);
    }

    @Override
    public List<TRowResult> getRowTs(ByteBuffer tableName, ByteBuffer row,
        long timestamp, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      return getRowWithColumnsTs(tableName, row, null,
                                 timestamp, attributes);
    }

    @Override
    public List<TRowResult> getRowWithColumnsTs(
        ByteBuffer tableName, ByteBuffer row, List<ByteBuffer> columns,
        long timestamp, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      try {
        HTable table = getTable(tableName);
        if (columns == null) {
          Get get = new Get(getBytes(row));
          addAttributes(get, attributes);
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          Result result = table.get(get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        Get get = new Get(getBytes(row));
        addAttributes(get, attributes);
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
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<TRowResult> getRows(ByteBuffer tableName,
                                    List<ByteBuffer> rows,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError {
      return getRowsWithColumnsTs(tableName, rows, null,
                                  HConstants.LATEST_TIMESTAMP,
                                  attributes);
    }

    @Override
    public List<TRowResult> getRowsWithColumns(ByteBuffer tableName,
                                               List<ByteBuffer> rows,
        List<ByteBuffer> columns,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, columns,
                                  HConstants.LATEST_TIMESTAMP,
                                  attributes);
    }

    @Override
    public List<TRowResult> getRowsTs(ByteBuffer tableName,
                                      List<ByteBuffer> rows,
        long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, null,
                                  timestamp, attributes);
    }

    @Override
    public List<TRowResult> getRowsWithColumnsTs(ByteBuffer tableName,
                                                 List<ByteBuffer> rows,
        List<ByteBuffer> columns, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      try {
        List<Get> gets = new ArrayList<Get>(rows.size());
        HTable table = getTable(tableName);
        if (metrics != null) {
          metrics.incNumRowKeysInBatchGet(rows.size());
        }
        for (ByteBuffer row : rows) {
          Get get = new Get(getBytes(row));
          addAttributes(get, attributes);
          if (columns != null) {

            for(ByteBuffer column : columns) {
              byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
              if (famAndQf.length == 1) {
                get.addFamily(famAndQf[0]);
              } else {
                get.addColumn(famAndQf[0], famAndQf[1]);
              }
            }
          }
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          gets.add(get);
        }
        Result[] result = table.get(gets);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void deleteAll(
        ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError {
      deleteAllTs(tableName, row, column, HConstants.LATEST_TIMESTAMP,
                  attributes);
    }

    @Override
    public void deleteAllTs(ByteBuffer tableName,
                            ByteBuffer row,
                            ByteBuffer column,
        long timestamp, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      try {
        HTable table = getTable(tableName);
        Delete delete  = new Delete(getBytes(row));
        addAttributes(delete, attributes);
        byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
        if (famAndQf.length == 1) {
          delete.deleteFamily(famAndQf[0], timestamp);
        } else {
          delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
        }
        table.delete(delete);

      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void deleteAllRow(
        ByteBuffer tableName, ByteBuffer row,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      deleteAllRowTs(tableName, row, HConstants.LATEST_TIMESTAMP, attributes);
    }

    @Override
    public void deleteAllRowTs(
        ByteBuffer tableName, ByteBuffer row, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      try {
        HTable table = getTable(tableName);
        Delete delete  = new Delete(getBytes(row), timestamp, null);
        addAttributes(delete, attributes);
        table.delete(delete);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
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
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      } catch (IllegalArgumentException e) {
        LOG.warn(e.getMessage(), e);
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
          throw new IOException("table does not exist");
        }
        admin.deleteTable(tableName);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void mutateRow(ByteBuffer tableName, ByteBuffer row,
        List<Mutation> mutations, Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, IllegalArgument {
      mutateRowTs(tableName, row, mutations, HConstants.LATEST_TIMESTAMP,
                  attributes);
    }

    @Override
    public void mutateRowTs(ByteBuffer tableName, ByteBuffer row,
        List<Mutation> mutations, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, IllegalArgument {
      HTable table = null;
      try {
        table = getTable(tableName);
        Put put = new Put(getBytes(row), timestamp, null);
        addAttributes(put, attributes);

        Delete delete = new Delete(getBytes(row));
        addAttributes(delete, attributes);
        if (metrics != null) {
          metrics.incNumRowKeysInBatchMutate(mutations.size());
        }

        // I apologize for all this mess :)
        for (Mutation m : mutations) {
          byte[][] famAndQf = KeyValue.parseColumn(getBytes(m.column));
          if (m.isDelete) {
            if (famAndQf.length == 1) {
              delete.deleteFamily(famAndQf[0], timestamp);
            } else {
              delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
            }
            delete.setWriteToWAL(m.writeToWAL);
          } else {
            if(famAndQf.length == 1) {
              put.add(famAndQf[0], HConstants.EMPTY_BYTE_ARRAY,
                  m.value != null ? getBytes(m.value)
                      : HConstants.EMPTY_BYTE_ARRAY);
            } else {
              put.add(famAndQf[0], famAndQf[1],
                  m.value != null ? getBytes(m.value)
                      : HConstants.EMPTY_BYTE_ARRAY);
            }
            put.setWriteToWAL(m.writeToWAL);
          }
        }
        if (!delete.isEmpty())
          table.delete(delete);
        if (!put.isEmpty())
          table.put(put);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      } catch (IllegalArgumentException e) {
        LOG.warn(e.getMessage(), e);
        throw new IllegalArgument(e.getMessage());
      }
    }

    @Override
    public void mutateRows(ByteBuffer tableName, List<BatchMutation> rowBatches,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, IllegalArgument, TException {
      mutateRowsTs(tableName, rowBatches, HConstants.LATEST_TIMESTAMP, attributes);
    }

    @Override
    public void mutateRowsTs(
        ByteBuffer tableName, List<BatchMutation> rowBatches, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, IllegalArgument, TException {
      List<Put> puts = new ArrayList<Put>();
      List<Delete> deletes = new ArrayList<Delete>();

      for (BatchMutation batch : rowBatches) {
        byte[] row = getBytes(batch.row);
        List<Mutation> mutations = batch.mutations;
        Delete delete = new Delete(row);
        addAttributes(delete, attributes);
        Put put = new Put(row, timestamp, null);
        addAttributes(put, attributes);
        for (Mutation m : mutations) {
          byte[][] famAndQf = KeyValue.parseColumn(getBytes(m.column));
          if (m.isDelete) {
            // no qualifier, family only.
            if (famAndQf.length == 1) {
              delete.deleteFamily(famAndQf[0], timestamp);
            } else {
              delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
            }
            delete.setWriteToWAL(m.writeToWAL);
          } else {
            if(famAndQf.length == 1) {
              put.add(famAndQf[0], HConstants.EMPTY_BYTE_ARRAY,
                  m.value != null ? getBytes(m.value)
                      : HConstants.EMPTY_BYTE_ARRAY);
            } else {
              put.add(famAndQf[0], famAndQf[1],
                  m.value != null ? getBytes(m.value)
                      : HConstants.EMPTY_BYTE_ARRAY);
            }
            put.setWriteToWAL(m.writeToWAL);
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
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      } catch (IllegalArgumentException e) {
        LOG.warn(e.getMessage(), e);
        throw new IllegalArgument(e.getMessage());
      }
    }

    @Deprecated
    @Override
    public long atomicIncrement(
        ByteBuffer tableName, ByteBuffer row, ByteBuffer column, long amount)
            throws IOError, IllegalArgument, TException {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return atomicIncrement(tableName, row, famAndQf[0], new byte[0],
            amount);
      }
      return atomicIncrement(tableName, row, famAndQf[0], famAndQf[1], amount);
    }

    protected long atomicIncrement(ByteBuffer tableName, ByteBuffer row,
        byte [] family, byte [] qualifier, long amount)
        throws IOError, IllegalArgument, TException {
      HTable table;
      try {
        table = getTable(tableName);
        return table.incrementColumnValue(
            getBytes(row), family, qualifier, amount);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    public void scannerClose(int id) throws IOError, IllegalArgument {
      LOG.debug("scannerClose: id=" + id);
      ResultScanner scanner = getScanner(id);
      if (scanner == null) {
        String message = "scanner ID is invalid";
        LOG.warn(message);
        throw new IllegalArgument("scanner ID is invalid");
      }
      scanner.close();
      removeScanner(id);
    }

    @Override
    public List<TRowResult> scannerGetList(int id,int nbRows)
        throws IllegalArgument, IOError {
      LOG.debug("scannerGetList: id=" + id);
      ResultScanner scanner = getScanner(id);
      if (null == scanner) {
        String message = "scanner ID is invalid";
        LOG.warn(message);
        throw new IllegalArgument("scanner ID is invalid");
      }

      Result [] results = null;
      try {
        results = scanner.next(nbRows);
        if (null == results) {
          return new ArrayList<TRowResult>();
        }
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
      return ThriftUtilities.rowResultFromHBase(results);
    }

    @Override
    public List<TRowResult> scannerGet(int id) throws IllegalArgument, IOError {
      return scannerGetList(id,1);
    }

    public int scannerOpenWithScan(ByteBuffer tableName, TScan tScan,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan();
        addAttributes(scan, attributes);
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
        if (tScan.isSetColumns() && tScan.getColumns().size() != 0) {
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
          scan.setFilter(
              parseFilter.parseFilterString(tScan.getFilterString()));
        }
        return addScanner(table.getScanner(scan));
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public int scannerOpen(ByteBuffer tableName, ByteBuffer startRow,
        List<ByteBuffer> columns,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow));
        addAttributes(scan, attributes);
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
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public int scannerOpenWithStop(ByteBuffer tableName, ByteBuffer startRow,
        ByteBuffer stopRow, List<ByteBuffer> columns,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow), getBytes(stopRow));
        addAttributes(scan, attributes);
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
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public int scannerOpenWithPrefix(ByteBuffer tableName,
                                     ByteBuffer startAndPrefix,
                                     List<ByteBuffer> columns,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startAndPrefix));
        addAttributes(scan, attributes);
        Filter f = new WhileMatchFilter(
            new PrefixFilter(getBytes(startAndPrefix)));
        scan.setFilter(f);
        if (columns != null && columns.size() != 0) {
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
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public int scannerOpenTs(ByteBuffer tableName, ByteBuffer startRow,
        List<ByteBuffer> columns, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow));
        addAttributes(scan, attributes);
        scan.setTimeRange(Long.MIN_VALUE, timestamp);
        if (columns != null && columns.size() != 0) {
          for (ByteBuffer column : columns) {
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
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public int scannerOpenWithStopTs(ByteBuffer tableName, ByteBuffer startRow,
        ByteBuffer stopRow, List<ByteBuffer> columns, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow), getBytes(stopRow));
        addAttributes(scan, attributes);
        scan.setTimeRange(Long.MIN_VALUE, timestamp);
        if (columns != null && columns.size() != 0) {
          for (ByteBuffer column : columns) {
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
        LOG.warn(e.getMessage(), e);
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
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<TCell> getRowOrBefore(ByteBuffer tableName, ByteBuffer row,
        ByteBuffer family) throws IOError {
      try {
        HTable table = getTable(getBytes(tableName));
        Result result = table.getRowOrBefore(getBytes(row), getBytes(family));
        return ThriftUtilities.cellFromHBase(result.raw());
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public TRegionInfo getRegionInfo(ByteBuffer searchRow) throws IOError {
      try {
        HTable table = getTable(HConstants.META_TABLE_NAME);
        byte[] row = toBytes(searchRow);
        Result startRowResult = table.getRowOrBefore(
          row, HConstants.CATALOG_FAMILY);

        if (startRowResult == null) {
          throw new IOException("Cannot find row in .META., row="
                                + Bytes.toString(searchRow.array()));
        }

        // find region start and end keys
        byte[] value = startRowResult.getValue(HConstants.CATALOG_FAMILY,
                                               HConstants.REGIONINFO_QUALIFIER);
        if (value == null || value.length == 0) {
          throw new IOException("HRegionInfo REGIONINFO was null or " +
                                " empty in Meta for row="
                                + Bytes.toString(row));
        }
        HRegionInfo regionInfo = Writables.getHRegionInfo(value);
        TRegionInfo region = new TRegionInfo();
        region.setStartKey(regionInfo.getStartKey());
        region.setEndKey(regionInfo.getEndKey());
        region.id = regionInfo.getRegionId();
        region.setName(regionInfo.getRegionName());
        region.version = regionInfo.getVersion();

        // find region assignment to server
        value = startRowResult.getValue(HConstants.CATALOG_FAMILY,
                                        HConstants.SERVER_QUALIFIER);
        if (value != null && value.length > 0) {
          String hostAndPort = Bytes.toString(value);
          region.setServerName(Bytes.toBytes(
              Addressing.parseHostname(hostAndPort)));
          region.port = Addressing.parsePort(hostAndPort);
        }
        return region;
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<TRowResult> parallelGet(ByteBuffer tableName, ByteBuffer column, List<ByteBuffer> rows) throws IOError, TException {
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
    public boolean queueIncrementColumnValues(List<org.apache.hadoop.hbase.thrift.generated.Increment> increments) throws TException {
      List<TIncrement> tincs = new ArrayList<TIncrement>(increments.size());
      for(org.apache.hadoop.hbase.thrift.generated.Increment i:increments) {
        tincs.add(ThriftUtilities.tiincrementFromThrift(i));
      }
      return this.coalescer.queueIncrements(tincs);
    }

    @Override
    public List<TRowResult> parallelScan(ScanSpec spec, List<ByteBuffer> startRows, List<ByteBuffer> endRows) throws IOError, TException {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ScanResult scan(ScanSpec spec, int nRows, boolean closeAfter) throws IOError, TException {
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

    private void initMetrics(ThriftMetrics metrics) {
      this.metrics = metrics;
    }

    @Override
    public void increment(TIncrement tincrement) throws IOError, TException {

      if (tincrement.getRow().length == 0 || tincrement.getTable().length == 0) {
        throw new TException("Must supply a table and a row key; can't increment");
      }

      if (conf.getBoolean(COALESCE_INC_KEY, false)) {
        this.coalescer.queueIncrement(tincrement);
        return;
      }

      try {
        HTable table = getTable(tincrement.getTable());
        org.apache.hadoop.hbase.client.Increment
            inc = ThriftUtilities.incrementFromThrift(tincrement);
        table.increment(inc);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void incrementRows(List<TIncrement> tincrements) throws IOError, TException {
      if (conf.getBoolean(COALESCE_INC_KEY, false)) {
        this.coalescer.queueIncrements(tincrements);
        return;
      }
      for (TIncrement tinc : tincrements) {
        increment(tinc);
      }
    }
  }



  /**
   * Adds all the attributes into the Operation object
   */
  private static void addAttributes(OperationWithAttributes op,
    Map<ByteBuffer, ByteBuffer> attributes) {
    if (attributes == null || attributes.size() == 0) {
      return;
    }
    for (Map.Entry<ByteBuffer, ByteBuffer> entry : attributes.entrySet()) {
      String name = Bytes.toStringBinary(getBytes(entry.getKey()));
      byte[] value =  getBytes(entry.getValue());
      op.setAttribute(name, value);
    }
  }

  public static void registerFilters(Configuration conf) {
    String[] filters = conf.getStrings("hbase.thrift.filters");
    if(filters != null) {
      for(String filterClass: filters) {
        String[] filterPart = filterClass.split(":");
        if(filterPart.length != 2) {
          LOG.warn("Invalid filter specification " + filterClass + " - skipping");
        } else {
          ParseFilter.registerFilter(filterPart[0], filterPart[1]);
        }
      }
    }
  }
}
