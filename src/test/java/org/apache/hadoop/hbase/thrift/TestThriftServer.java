/*
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.thrift;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.ScanResult;
import org.apache.hadoop.hbase.thrift.generated.ScanSpec;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;

/**
 * Unit testing for ThriftServer.HBaseHandler, a part of the
 * org.apache.hadoop.hbase.thrift package.
 */
public class TestThriftServer extends HBaseClusterTestCase {
  public static final Log LOG = LogFactory.getLog(TestThriftServer.class);

  private static ByteBuffer $bb(String i) {
    return ByteBuffer.wrap(Bytes.toBytes(i));
  }
  // Static names for tables, columns, rows, and values
  private static ByteBuffer tableAname = $bb("tableA");
  private static ByteBuffer tableBname = $bb("tableB");
  private static ByteBuffer columnAname = $bb("columnA:");
  private static ByteBuffer columnBname = $bb("columnB:");
  private static ByteBuffer rowAname = $bb("rowA");
  private static ByteBuffer rowBname = $bb("rowB");
  private static ByteBuffer valueAname = $bb("valueA");
  private static ByteBuffer valueBname = $bb("valueB");
  private static ByteBuffer valueCname = $bb("valueC");
  private static ByteBuffer valueDname = $bb("valueD");

  /**
   * Runs all of the tests under a single JUnit test method.  We
   * consolidate all testing to one method because HBaseClusterTestCase
   * is prone to OutOfMemoryExceptions when there are three or more
   * JUnit test methods.
   *
   * @throws Exception
   */
  public void testAll() throws Exception {
    // Run all tests.  Tests will fail if not run in the right order.
    doTestTableCreateDrop();
    doTestTableMutations();
    doTestTableTimestampsAndColumns();
    doTestTableScanners();
    doTestOneShotScan();
  }

  /**
   * Test scan in one shot.
   * @throws Exception
   */
  public void doTestOneShotScan() throws Exception {
    ByteBuffer oneshotscan = $bb("oneshotscan");
    ThriftServer.HBaseHandler handler = new ThriftServer.HBaseHandler(this.conf);
    handler.createTable(oneshotscan, getColumnDescriptors());
    final int rowCount = 20;
    for (int i = 0; i < rowCount; i++) {
      // Add to tableAname on rowAname:
      // columnA -> valueA
      // columnB -> valueB
      handler.mutateRow(oneshotscan, $bb("row" + (i < 10? "0" + i: i)), getMutations());
    }
    ScanSpec spec = new ScanSpec();
    spec.setColumns(getColumnList(true, true));
    spec.setTableName(oneshotscan);
    ScanResult results = handler.scan(spec, 10, false);
    for (TRowResult result: results.getResults()) {
      LOG.info(Bytes.toString(result.getRow()));
    }
    int count = results.getResultsSize();
    count += doHasMoreRows(handler, results.getScannerId(), 3);
    count += doHasMoreRows(handler, results.getScannerId(), 3);
    count += doHasMoreRows(handler, results.getScannerId(), 10);
    assertEquals(rowCount, count);
  }

  /**
   * @param handler
   * @param scannerid
   * @param num
   * @return Count of how many rows fetched.
   * @throws IOError
   * @throws TException
   */
  private int doHasMoreRows(final ThriftServer.HBaseHandler handler,
      final int scannerid, final int num)
  throws IOError, TException {
    ScanResult results = handler.scanMore(scannerid, num, false);
    for (TRowResult result: results.getResults()) {
      LOG.info(Bytes.toString(result.getRow()));
    }
    return results.getResultsSize();
  }

  /**
   * Tests for creating, enabling, disabling, and deleting tables.  Also
   * tests that creating a table with an invalid column name yields an
   * IllegalArgument exception.
   *
   * @throws Exception
   */
  public void doTestTableCreateDrop() throws Exception {
    ThriftServer.HBaseHandler handler = new ThriftServer.HBaseHandler(this.conf);

    // Create/enable/disable/delete tables, ensure methods act correctly
    assertEquals(handler.getTableNames().size(), 0);
    handler.createTable(tableAname, getColumnDescriptors());
    assertEquals(handler.getTableNames().size(), 1);
    assertEquals(handler.getColumnDescriptors(tableAname).size(), 2);
    assertTrue(handler.isTableEnabled(tableAname));
    handler.createTable(tableBname, new ArrayList<ColumnDescriptor>());
    assertEquals(handler.getTableNames().size(), 2);
    handler.disableTable(tableBname);
    assertFalse(handler.isTableEnabled(tableBname));
    handler.deleteTable(tableBname);
    assertEquals(handler.getTableNames().size(), 1);
    handler.disableTable(tableAname);
    /* TODO Reenable.
    assertFalse(handler.isTableEnabled(tableAname));
    handler.enableTable(tableAname);
    assertTrue(handler.isTableEnabled(tableAname));
    handler.disableTable(tableAname);*/
    handler.deleteTable(tableAname);
  }

  /**
   * Tests adding a series of Mutations and BatchMutations, including a
   * delete mutation.  Also tests data retrieval, and getting back multiple
   * versions.
   *
   * @throws Exception
   */
  public void doTestTableMutations() throws Exception {
    // Setup
    ThriftServer.HBaseHandler handler = new ThriftServer.HBaseHandler(this.conf);
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply a few Mutations to rowA
    //     mutations.add(new Mutation(false, columnAname, valueAname));
    //     mutations.add(new Mutation(false, columnBname, valueBname));
    handler.mutateRow(tableAname, rowAname, getMutations());

    // Assert that the changes were made
    assertEquals(valueAname,
      handler.get(tableAname, rowAname, columnAname).get(0).value);
    TRowResult rowResult1 = handler.getRow(tableAname, rowAname).get(0);
    assertEquals(rowAname, rowResult1.row);
    assertEquals(valueBname,
      rowResult1.columns.get(columnBname).value);

    // Apply a few BatchMutations for rowA and rowB
    // rowAmutations.add(new Mutation(true, columnAname, null));
    // rowAmutations.add(new Mutation(false, columnBname, valueCname));
    // batchMutations.add(new BatchMutation(rowAname, rowAmutations));
    // Mutations to rowB
    // rowBmutations.add(new Mutation(false, columnAname, valueCname));
    // rowBmutations.add(new Mutation(false, columnBname, valueDname));
    // batchMutations.add(new BatchMutation(rowBname, rowBmutations));
    handler.mutateRows(tableAname, getBatchMutations());

    // Assert that changes were made to rowA
    List<TCell> cells = handler.get(tableAname, rowAname, columnAname);
    assertFalse(cells.size() > 0);
    assertEquals(valueCname, handler.get(tableAname, rowAname, columnBname).get(0).value);
    List<TCell> versions = handler.getVer(tableAname, rowAname, columnBname, MAXVERSIONS);
    assertEquals(valueCname, versions.get(0).value);
    assertEquals(valueBname, versions.get(1).value);

    // Assert that changes were made to rowB
    TRowResult rowResult2 = handler.getRow(tableAname, rowBname).get(0);
    assertEquals(rowBname, rowResult2.row);
    assertEquals(valueCname, rowResult2.columns.get(columnAname).value);
	  assertEquals(valueDname, rowResult2.columns.get(columnBname).value);

    // Apply some deletes
    handler.deleteAll(tableAname, rowAname, columnBname);
    handler.deleteAllRow(tableAname, rowBname);

    // Assert that the deletes were applied
    int size = handler.get(tableAname, rowAname, columnBname).size();
    assertEquals(0, size);
    size = handler.getRow(tableAname, rowBname).size();
    assertEquals(0, size);

    // Teardown
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }

  /**
   * Similar to testTableMutations(), except Mutations are applied with
   * specific timestamps and data retrieval uses these timestamps to
   * extract specific versions of data.
   *
   * @throws Exception
   */
  public void doTestTableTimestampsAndColumns() throws Exception {
    // Setup
    ThriftServer.HBaseHandler handler = new ThriftServer.HBaseHandler(this.conf);
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply timestamped Mutations to rowA
    long time1 = System.currentTimeMillis();
    handler.mutateRowTs(tableAname, rowAname, getMutations(), time1);

    Thread.sleep(1000);

    // Apply timestamped BatchMutations for rowA and rowB
    long time2 = System.currentTimeMillis();
    handler.mutateRowsTs(tableAname, getBatchMutations(), time2);

    // Apply an overlapping timestamped mutation to rowB
    handler.mutateRowTs(tableAname, rowBname, getMutations(), time2);

    // the getVerTs is [-inf, ts) so you need to increment one.
    time1 += 1;
    time2 += 2;

    // Assert that the timestamp-related methods retrieve the correct data
    assertEquals(2, handler.getVerTs(tableAname, rowAname, columnBname, time2,
      MAXVERSIONS).size());
    assertEquals(1, handler.getVerTs(tableAname, rowAname, columnBname, time1,
      MAXVERSIONS).size());

    TRowResult rowResult1 = handler.getRowTs(tableAname, rowAname, time1).get(0);
    TRowResult rowResult2 = handler.getRowTs(tableAname, rowAname, time2).get(0);
    // columnA was completely deleted
    //assertTrue(Bytes.equals(rowResult1.columns.get(columnAname).value, valueAname));
    assertEquals(rowResult1.columns.get(columnBname).value, valueBname);
    assertEquals(rowResult2.columns.get(columnBname).value, valueCname);

    // ColumnAname has been deleted, and will never be visible even with a getRowTs()
    assertFalse(rowResult2.columns.containsKey(columnAname));

    List<ByteBuffer> columns = new ArrayList<ByteBuffer>();
    columns.add(columnBname);

    rowResult1 = handler.getRowWithColumns(tableAname, rowAname, columns).get(0);
    assertEquals(rowResult1.columns.get(columnBname).value, valueCname);
    assertFalse(rowResult1.columns.containsKey(columnAname));

    rowResult1 = handler.getRowWithColumnsTs(tableAname, rowAname, columns, time1).get(0);
    assertEquals(rowResult1.columns.get(columnBname).value, valueBname);
    assertFalse(rowResult1.columns.containsKey(columnAname));

    // query using the getRows() api:
    List<ByteBuffer> rows = new ArrayList<ByteBuffer>();
    rows.add(rowAname); rows.add(rowBname);
    List<TRowResult> r = handler.getRows(tableAname, rows);
    assertEquals(2, r.size());

    r = handler.getRowsWithColumns(tableAname,  rows, columns);
    assertEquals(2, r.size());

    // Apply some timestamped deletes
    // this actually deletes _everything_.
    // nukes everything in columnB: forever.
    handler.deleteAllTs(tableAname, rowAname, columnBname, time1);
    handler.deleteAllRowTs(tableAname, rowBname, time2);

    // Assert that the timestamp-related methods retrieve the correct data
    int size = handler.getVerTs(tableAname, rowAname, columnBname, time1, MAXVERSIONS).size();
    assertEquals(0, size);

    size = handler.getVerTs(tableAname, rowAname, columnBname, time2, MAXVERSIONS).size();
    assertEquals(1, size);

    // should be available....
    assertEquals(handler.get(tableAname, rowAname, columnBname).get(0).value, valueCname);

    assertEquals(0, handler.getRow(tableAname, rowBname).size());

    // Teardown
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }

  /**
   * Tests the four different scanner-opening methods (with and without
   * a stoprow, with and without a timestamp).
   *
   * @throws Exception
   */
  public void doTestTableScanners() throws Exception {
    // Setup
    ThriftServer.HBaseHandler handler = new ThriftServer.HBaseHandler(this.conf);
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply timestamped Mutations to rowA
    long time1 = System.currentTimeMillis();
    // Add to tableAname on rowAname:
    // columnA -> valueA
    // columnB -> valueB
    handler.mutateRowTs(tableAname, rowAname, getMutations(), time1);

    // Sleep to assure that 'time1' and 'time2' will be different even with a
    // coarse grained system timer.
    Thread.sleep(1000);

    // Apply timestamped BatchMutations for rowA and rowB
    // Add to tableAname
    //
    // At rowA, columnA -> DELETE
    // At rowA, columnB -> valueC
    // At rowB, columnA -> valueC
    // At rowB, columnB -> valueD
    long time2 = System.currentTimeMillis();
    handler.mutateRowsTs(tableAname, getBatchMutations(), time2);

    LOG.info("time1=" + time1 + ", time2=" + time2 + ", time1 + 1=" + (time1 + 1));
    time1 += 1;

    // Test a scanner on all rows and all columns, no timestamp
    int scanner1 =
      handler.scannerOpen(tableAname, rowAname, getColumnList(true, true));
    // Get row A.
    TRowResult rowResult1a = handler.scannerGet(scanner1).get(0);
    assertEquals(rowResult1a.row, rowAname);
    assertEquals(rowResult1a.columns.size(), 1);
    assertEquals(rowResult1a.columns.get(columnBname).value, valueCname);
    // Get next row, row B.
    TRowResult rowResult1b = handler.scannerGet(scanner1).get(0);
    assertEquals(rowResult1b.row, rowBname);
    assertEquals(rowResult1b.columns.size(), 2);
    assertEquals(rowResult1b.columns.get(columnAname).value, valueCname);
    assertEquals(rowResult1b.columns.get(columnBname).value, valueDname);
    closeScanner(scanner1, handler);

    // Check w/ raw client.
    HTable t = new HTable(this.conf, tableAname.array());
    Scan s = new Scan(rowAname.array());
    List<ByteBuffer> columns = getColumnList(true, true);
    for (ByteBuffer family: columns) s.addFamily(KeyValue.parseColumn(family.array())[0]);
    s.setTimeRange(time1, Long.MAX_VALUE);
    ResultScanner rs = t.getScanner(s);
    while(true) {
      Result r = rs.next();
      if (r == null) break;
      LOG.info("Result=" + r);
      for (KeyValue kv: r.raw()) {
        LOG.info("kv=" + kv.toString() + ", value=" +
          Bytes.toStringBinary(kv.getValue()));
      }
    }
    rs.close();

    // Test a scanner on all rows and all columns, with timestamp. The below
    // asks that we return values newer or equal to time1.
    int scanner2 =
      handler.scannerOpenTs(tableAname, rowAname, getColumnList(true, true), time1);
    TRowResult rowResult2a = handler.scannerGet(scanner2).get(0);
    assertEquals(rowResult2a.columns.size(), 1);
    LOG.info("Value=" +
      Bytes.toStringBinary(rowResult2a.columns.get(columnBname).value.array()));
    // First row is A.  It has a deleted colA and a valueC in colB.
    assertEquals(rowResult2a.columns.get(columnBname).value, valueCname);
    rowResult2a = handler.scannerGet(scanner2).get(0);
    assertEquals(rowResult2a.columns.size(), 2);
    assertEquals(rowResult2a.columns.get(columnAname).value, valueCname);
    assertEquals(rowResult2a.columns.get(columnBname).value, valueDname);
    closeScanner(scanner2, handler);

    // Test a scanner on the first row and first column only, no timestamp
    int scanner3 = handler.scannerOpenWithStop(tableAname, rowAname, rowBname,
        getColumnList(true, false));
    closeScanner(scanner3, handler);

    // Test a scanner on the first row and second column only, with timestamp
    int scanner4 = handler.scannerOpenWithStopTs(tableAname, rowAname, rowBname,
        getColumnList(false, true), time1);
    TRowResult rowResult4a = handler.scannerGet(scanner4).get(0);
    assertEquals(rowResult4a.columns.size(), 1);
    assertEquals(rowResult4a.columns.get(columnBname).value, valueBname);

    // Teardown
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }

  /**
   *
   * @return a List of ColumnDescriptors for use in creating a table.  Has one
   * default ColumnDescriptor and one ColumnDescriptor with fewer versions
   */
  private List<ColumnDescriptor> getColumnDescriptors() {
    ArrayList<ColumnDescriptor> cDescriptors = new ArrayList<ColumnDescriptor>();

    // A default ColumnDescriptor
    ColumnDescriptor cDescA = new ColumnDescriptor();
    cDescA.name = columnAname;
    cDescriptors.add(cDescA);

    // A slightly customized ColumnDescriptor (only 2 versions)
    ColumnDescriptor cDescB = new ColumnDescriptor(columnBname, 2, "NONE",
        false, "NONE", 0, 0, false, -1);
    cDescriptors.add(cDescB);

    return cDescriptors;
  }

  /**
   *
   * @param includeA whether or not to include columnA
   * @param includeB whether or not to include columnB
   * @return a List of column names for use in retrieving a scanner
   */
  private List<ByteBuffer> getColumnList(boolean includeA, boolean includeB) {
    List<ByteBuffer> columnList = new ArrayList<ByteBuffer>();
    if (includeA) columnList.add(columnAname);
    if (includeB) columnList.add(columnBname);
    return columnList;
  }

  /**
   *
   * @return a List of Mutations for a row, with columnA having valueA
   * and columnB having valueB
   */
  private List<Mutation> getMutations() {
    List<Mutation> mutations = new ArrayList<Mutation>();
    mutations.add(new Mutation(false, columnAname, valueAname));
    mutations.add(new Mutation(false, columnBname, valueBname));
    return mutations;
  }

  /**
   *
   * @return a List of BatchMutations with the following effects:
   * (rowA, columnA): delete
   * (rowA, columnB): place valueC
   * (rowB, columnA): place valueC
   * (rowB, columnB): place valueD
   */
  private List<BatchMutation> getBatchMutations() {
    List<BatchMutation> batchMutations = new ArrayList<BatchMutation>();

    // Mutations to rowA.  You can't mix delete and put anymore.
    List<Mutation> rowAmutations = new ArrayList<Mutation>();
    rowAmutations.add(new Mutation(true, columnAname, null));
    batchMutations.add(new BatchMutation(rowAname, rowAmutations));

    rowAmutations = new ArrayList<Mutation>();
    rowAmutations.add(new Mutation(false, columnBname, valueCname));
    batchMutations.add(new BatchMutation(rowAname, rowAmutations));

    // Mutations to rowB
    List<Mutation> rowBmutations = new ArrayList<Mutation>();
    rowBmutations.add(new Mutation(false, columnAname, valueCname));
    rowBmutations.add(new Mutation(false, columnBname, valueDname));
    batchMutations.add(new BatchMutation(rowBname, rowBmutations));

    return batchMutations;
  }

  /**
   * Asserts that the passed scanner is exhausted, and then closes
   * the scanner.
   *
   * @param scannerId the scanner to close
   * @param handler the HBaseHandler interfacing to HBase
   * @throws Exception
   */
  private void closeScanner(int scannerId, ThriftServer.HBaseHandler handler) throws Exception {
    handler.scannerGet(scannerId);
    handler.scannerClose(scannerId);
  }
}
