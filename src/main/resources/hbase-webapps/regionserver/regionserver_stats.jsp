<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.*"
  import="org.apache.hadoop.io.Text"
  import="org.json.simple.JSONObject"
  import="org.json.simple.JSONArray"
  import="java.lang.management.ManagementFactory"
  import="java.lang.management.MemoryUsage"
  import="org.apache.hadoop.hbase.regionserver.HRegionServer"
  import="org.apache.hadoop.hbase.regionserver.HRegion"
  import="org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics"
  import="org.apache.hadoop.hbase.util.Bytes"
  import="org.apache.hadoop.hbase.HConstants"
  import="org.apache.hadoop.hbase.HServerInfo"
  import="org.apache.hadoop.hbase.HServerLoad"
  import="org.apache.hadoop.hbase.HRegionInfo" %><%--
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
  HRegionServer regionServer = (HRegionServer)getServletContext().getAttribute(HRegionServer.REGIONSERVER);
  HServerInfo serverInfo = regionServer.getHServerInfo();
  RegionServerMetrics metrics = regionServer.getMetrics();
  Collection<HRegionInfo> onlineRegions = regionServer.getSortedOnlineRegionInfos();
  int interval = regionServer.getConfiguration().getInt("hbase.regionserver.msginterval", 3000)/1000;

JSONObject json = new JSONObject();

JSONObject regionServerAttr = new JSONObject();

regionServerAttr.put("requests", metrics.getRequests());
regionServerAttr.put("regions", metrics.regions.get());
regionServerAttr.put("stores", metrics.stores.get());
regionServerAttr.put("storefiles", metrics.storefiles.get());
regionServerAttr.put("storefileIndexSize", metrics.storefileIndexSizeMB.get());
regionServerAttr.put("memstoreSize", metrics.memstoreSizeMB.get());

int MB = 1024*1024;
MemoryUsage memory =
      ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
regionServerAttr.put("usedHeap", memory.getUsed()/MB );
regionServerAttr.put("maxHeap", memory.getMax()/MB);
regionServerAttr.put("blockCacheSize", metrics.blockCacheSize.get());
regionServerAttr.put("blockCacheFree", metrics.blockCacheFree.get());
regionServerAttr.put("blockCacheCount", metrics.blockCacheCount.get());
regionServerAttr.put("blockCacheHitRatio", metrics.blockCacheHitRatio.get());


json.put("region_server_attributes", regionServerAttr);

JSONArray onlineRegionsJson = new JSONArray();
if (onlineRegions != null && onlineRegions.size() > 0) {
	for (HRegionInfo r: onlineRegions) {
		HServerLoad.RegionLoad load = regionServer.createRegionLoad(r.getRegionName());

		JSONObject region = new JSONObject();
		region.put("name", r.getRegionNameAsString());
		region.put("encoded_name", r.getEncodedName());
		region.put("start_key", Bytes.toStringBinary(r.getStartKey()));
		region.put("end_key",   Bytes.toStringBinary(r.getEndKey()));
		region.put("stores", load.getStores());
		region.put("storefiles", load.getStorefiles());
		//region.put("memstoreSize", load.getMemcacheSizeMB());
		region.put("storefileIndexSize", load.getStorefileIndexSizeMB());
		
		onlineRegionsJson.add(region);
	}
}
json.put("online_regions", onlineRegionsJson);

%><%= json.toString() %>
