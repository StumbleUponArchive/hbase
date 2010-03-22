<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.*"
  import="java.net.URLEncoder"
  import="org.json.simple.JSONObject"
  import="org.json.simple.JSONArray"
  import="org.apache.hadoop.io.Text"
  import="org.apache.hadoop.hbase.util.Bytes"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.HConstants"
  import="org.apache.hadoop.hbase.master.MetaRegion"
  import="org.apache.hadoop.hbase.client.HBaseAdmin"
  import="org.apache.hadoop.hbase.io.ImmutableBytesWritable"
  import="org.apache.hadoop.hbase.HServerInfo"
  import="org.apache.hadoop.hbase.HServerAddress"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.HColumnDescriptor"
  import="org.apache.hadoop.hbase.HTableDescriptor" %>
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
  HServerAddress rootLocation = master.getRegionManager().getRootRegionLocation();
  Map<byte [], MetaRegion> onlineRegions = master.getRegionManager().getOnlineMetaRegions();
  Map<String, HServerInfo> serverToServerInfos =
    master.getServerManager().getServersToServerInfo();
  int interval = conf.getInt("hbase.regionserver.msginterval", 3000)/1000;
  if (interval == 0) {
      interval = 1;
  }



JSONObject json = new JSONObject();
json.put("master_host", master.getMasterAddress().getHostname());
json.put("master_port", master.getMasterAddress().getPort());

JSONObject masterAttr = new JSONObject();
masterAttr.put("load_avg", master.getAverageLoad());
masterAttr.put("regions_on_fs", master.getRegionManager().countRegionsOnFS());
masterAttr.put("zookeeper_quorum", master.getZooKeeperWrapper().getQuorumServers());

json.put("master_attributes", masterAttr);

JSONArray userTables = new JSONArray();
HTableDescriptor[] tables = new HBaseAdmin(conf).listTables();
if(tables != null && tables.length > 0) {
	for(HTableDescriptor htDesc : tables ) {
		userTables.add(htDesc.getNameAsString());
	}
}

json.put("user_tables", userTables);

JSONArray regionServers = new JSONArray();
int totalRegions = 0;
int totalRequests = 0;
if (serverToServerInfos != null && serverToServerInfos.size() > 0) {
	String[] serverNames = serverToServerInfos.keySet().toArray(new String[serverToServerInfos.size()]);
	Arrays.sort(serverNames);
	for (String serverName: serverNames) {
		HServerInfo hsi = serverToServerInfos.get(serverName);
		String hostname = hsi.getServerAddress().getHostname() + ":" + hsi.getInfoPort();
		String url = "http://" + hostname + "/";
		int requests = hsi.getLoad().getNumberOfRequests() / interval;
		int regions  = hsi.getLoad().getNumberOfRegions();
		totalRegions += hsi.getLoad().getNumberOfRegions();
		totalRequests += hsi.getLoad().getNumberOfRequests() / interval;
		long startCode = hsi.getStartCode();  

		JSONObject obj = new JSONObject();
		obj.put("hosts", hsi.getServerAddress().getHostname());
		obj.put("port",  hsi.getInfoPort());
		obj.put("requests", requests);
		obj.put("regions",  regions);

		regionServers.add(obj);
	}
}
json.put("region_servers", regionServers);
json.put("total_region_servers", serverToServerInfos.size());
json.put("total_requests", totalRequests);
json.put("total_regions", totalRegions);

json.put("region_servers", regionServers);

%><%= json.toString() %>
