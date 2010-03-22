<%@ page contentType="text/html;charset=UTF-8"
  import="java.io.IOException"
  import="org.json.simple.JSONObject"
  import="org.json.simple.JSONArray"
  import="org.apache.hadoop.hbase.client.HBaseAdmin"
  import="org.apache.hadoop.hbase.client.HConnection"
  import="org.apache.hadoop.hbase.HRegionInfo"
  import="org.apache.zookeeper.ZooKeeper"
  import="org.apache.hadoop.hbase.HServerAddress"
  import="org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.master.HMaster" 
  import="org.apache.hadoop.hbase.HConstants"%>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%--
~ Licensed to the Apache Software Foundation (ASF) under one
~ or more contributor license agreements.  See the NOTICE file
~ distributed with this work for additional information
~ regarding copyright ownership.  The ASF licenses this file
~ to you under the Apache License, Version 2.0 (the
~ "License"); you may not use this file except in compliance
~ with the License.  You may obtain a copy of the License at
~
~     http://www.apache.org/licenses/LICENSE-2.0
~
~ Unless required by applicable law or agreed to in writing, software
~ distributed under the License is distributed on an "AS IS" BASIS,
~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
~ See the License for the specific language governing permissions and
~ limitations under the License.
--%>

<%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
  HBaseAdmin hbadmin = new HBaseAdmin(conf);
  HConnection connection = hbadmin.getConnection();
  ZooKeeperWrapper wrapper = connection.getZooKeeperWrapper();
  ZooKeeper zooKeeper = wrapper.getZooKeeper();
  
  JSONObject json = new JSONObject();
  json.put("is_cluster_up", zooKeeper.exists(wrapper.clusterStateZNode, null) != null);
  json.put("master_address", wrapper.readMasterAddress(null).toString());
  json.put("region_server_holding_root", wrapper.readRootRegionLocation().toString());
  json.put("quorum_servers", wrapper.getQuorumServers());

  JSONArray regionServers = new JSONArray();
  for (HServerAddress address : wrapper.scanRSDirectory()) {
      regionServers.add(address.toString());
  }
  json.put("region_servers", regionServers);
  json.put("master_election_znode", wrapper.getMasterElectionZNode());

%><%= json.toString() %>