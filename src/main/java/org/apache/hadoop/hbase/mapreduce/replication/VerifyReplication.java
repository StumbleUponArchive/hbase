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
package org.apache.hadoop.hbase.mapreduce.replication;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;


public class VerifyReplication {

  private static final Log LOG =
      LogFactory.getLog(VerifyReplication.class);

  public final static String NAME = "verifyrep";
  static long startTime = 0;
  static long endTime = 0;
  static String tableName = null;
  static String families = null;
  static String peerId = null;

  /**
   * Write table content out to files in hdfs.
   */
  static class Verifier extends TableMapper<ImmutableBytesWritable, Put> {

    public static enum Counters {GOODROWS, BADROWS}

    private ResultScanner replicatedScanner;
    /**
     * @param row  The current table row key.
     * @param value  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     */
    @Override
    public void map(ImmutableBytesWritable row, Result value,
                    Context context)
        throws IOException {
      if (replicatedScanner == null) {
        Configuration conf = context.getConfiguration();
        ReplicationAdmin repAdmin = new ReplicationAdmin(conf);
        Scan scan = new Scan();
        scan.setCaching(30);
        long startTime = conf.getLong(NAME+".startTime", 0);
        long endTime = conf.getLong(NAME+".endTime", 0);
        String families = conf.get(NAME+".families", null);
        if(families != null) {
          String[] fams = families.split(",");
          for(String fam : fams) {
            scan.addFamily(Bytes.toBytes(fam));
          }
        }
        if (startTime != 0) {
          scan.setTimeRange(startTime,
              endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
        }

        ReplicationPeer peer = repAdmin.getZkWrapper().getPeer(conf.get(NAME+".peerId"));
        HTable replicatedTable = new HTable(peer.getConfiguration(), conf.get(NAME+".tableName"));
        scan.setStartRow(value.getRow());
        replicatedScanner = replicatedTable.getScanner(scan);
      }
      Result res = replicatedScanner.next();
      try {
        ReplicationAdmin.compareResult(value, res);
        context.getCounter(Counters.GOODROWS).increment(1);
      } catch (Exception e) {
        LOG.warn("Bad row", e);
        context.getCounter(Counters.BADROWS).increment(1);
      }
    }

    protected void cleanup(Context context) {
      replicatedScanner.close();
    }
  }

  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws java.io.IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
  throws IOException {
    if (!doCommandLine(args)) {
      return null;
    }
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(VerifyReplication.class);
    job.getConfiguration().set(NAME+".peerId", peerId);
    job.getConfiguration().set(NAME+".tableName", tableName);
    job.getConfiguration().setLong(NAME+".startTime", startTime);
    job.getConfiguration().setLong(NAME+".endTime", endTime);
    job.getConfiguration().set(NAME+".families", families);
    Scan scan = new Scan();
    if (startTime != 0) {
      scan.setTimeRange(startTime,
          endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
    }
    if(families != null) {
      String[] fams = families.split(",");
      for(String fam : fams) {
        scan.addFamily(Bytes.toBytes(fam));
      }
    }
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
        Verifier.class, null, null, job);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    return job;
  }

  private static boolean doCommandLine(final String[] args) {
    if (args.length < 2) {
      printUsage(null);
      return false;
    }
    try {
      for (int i = 0; i < args.length; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage(null);
          return false;
        }

        final String startTimeArgKey = "--starttime=";
        if (cmd.startsWith(startTimeArgKey)) {
          startTime = Long.parseLong(cmd.substring(startTimeArgKey.length()));
          continue;
        }

        final String endTimeArgKey = "--endtime=";
        if (cmd.startsWith(endTimeArgKey)) {
          endTime = Long.parseLong(cmd.substring(endTimeArgKey.length()));
          continue;
        }

        final String familiesArgKey = "--families=";
        if (cmd.startsWith(familiesArgKey)) {
          families = cmd.substring(familiesArgKey.length());
          continue;
        }

        if (i == args.length-2) {
          peerId = cmd;
        }

        if (i == args.length-1) {
          tableName = cmd;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      printUsage("Can't start because " + e.getMessage());
      return false;
    }
    return true;
  }

    /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void printUsage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: verifyreplication [--starttime=X] [--stoptime=Y] " +
        "<peerid> <tablename>");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" starttime    beginning of the time range");
    System.err.println("              without endtime means from starttime to forever");
    System.err.println(" stoptime     end of the time range");
    System.err.println(" families     comma-separated list of families to copy");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" peerid       Id of the peer used for verification");
    System.err.println(" tablename    Name of the table to verify");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" To verify the data replicated from TestTable for a 1 hour window with peer #5 ");
    System.err.println(" $ bin/hbase " +
        "org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication --starttime=1265875194289 --stoptime=1265878794289 " +
        "5 TestTable ");
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Job job = createSubmittableJob(conf, args);
    if (job != null) {
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }
}
