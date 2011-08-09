package org.apache.hadoop.hbase.thrift;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.thrift.generated.Increment;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"org.w3c.*",
    "javax.xml.*",
    "javax.management.*",
    "com.sun.org.apache.xerces.*",
    "org.apache.log4j.*",
    "org.apache.commons.logging.*"})
@PrepareForTest({ThriftServer.HBaseHandler.class, HBaseConfiguration.class})


public class TestThriftServerCoalescing extends TestCase {

  private static ByteBuffer $bb(String s) {
    return ByteBuffer.wrap(Bytes.toBytes(s));
  }

  private static final ByteBuffer tableA = $bb("tableA");
  private static final ByteBuffer row1 = $bb("row1");

  private static final byte[] family1 = Bytes.toBytes("family1");
  private static final byte[] qual1 = Bytes.toBytes("qual1");

  private static final byte[] colon = Bytes.toBytes(":");

  private ThreadPoolExecutor pool;
  private ConcurrentHashMap countersMap;
  private BlockingQueue queue;
  private ThriftServer.HBaseHandler handler;

  @Override
  public void setUp() throws Exception {
    HBaseAdmin hbaseAdminMocked = mock(HBaseAdmin.class);
    PowerMockito.whenNew(HBaseAdmin.class).withArguments(any()).
        thenReturn(hbaseAdminMocked);
    PowerMockito.suppress(HBaseConfiguration.class.
        getDeclaredMethod("checkDefaultsVersion", Configuration.class));

    this.countersMap = mock(ConcurrentHashMap.class);
    PowerMockito.whenNew(ConcurrentHashMap.class).withArguments(
        Matchers.<Object>any(),
        Matchers.<Object>any(),
        Matchers.<Object>any()).
        thenReturn(countersMap);

    this.pool = mock(ThreadPoolExecutor.class);
    PowerMockito.whenNew(ThreadPoolExecutor.class).
        withParameterTypes(Integer.TYPE, Integer.TYPE,
            Long.TYPE, TimeUnit.class,
            BlockingQueue.class, ThreadFactory.class)
        .withArguments(any(), any(), any(), any(), any(), any())
        .thenReturn(pool);

    this.queue = mock(BlockingQueue.class);
    when(pool.getQueue()).thenReturn(this.queue);

    handler = new ThriftServer.HBaseHandler();
  }

  public void testCoalescing() throws Exception {
    List<Increment> incrs = new ArrayList<Increment>();
    incrs.add(new Increment(tableA, row1,
        ByteBuffer.wrap(Bytes.add(family1, colon, qual1)), 1));
    KeyValue kv = new KeyValue(row1.array(), family1, qual1, tableA.array());
    HashSet<KeyValue> hs = new HashSet<KeyValue>();
    hs.add(kv);
    Long counterRow1 = new Long(1);
    Long counterRow1Incremented = new Long(2);

    // First test, increment a counter that's already present in the map

    when(queue.size()).thenReturn(1);

    when(countersMap.remove(Matchers.<Object>any())).
        thenReturn(counterRow1).
        thenReturn(counterRow1Incremented);
    when(countersMap.keySet()).thenReturn(hs);

    // make the handler a spy so we can inject our own special HTable.
    handler = spy(handler);
    assertTrue(handler.queueIncrementColumnValues(incrs));

    ArgumentCaptor<Callable> captor = ArgumentCaptor.forClass(Callable.class);
    verify(pool).submit(captor.capture());
    Callable c = captor.getValue();

    HTable htm = mock(HTable.class);
    doReturn(htm).when(handler).getTable(Matchers.<byte[]>any());
    c.call();

    verify(htm).incrementColumnValue(
        row1.array(), family1, qual1, 2);

    assertEquals(1, handler.getSuccessfulCoalescings());

    // Second test, the counter is new but then when we want to add it back
    // something is already present, it spins and then it's able to put it in

    counterRow1 = new Long(1);
    when(queue.size()).thenReturn(1);
    when(countersMap.remove(Matchers.<Object>any())).
        thenReturn(null).
        thenReturn(null).
        thenReturn(counterRow1);
    when(countersMap.putIfAbsent(Matchers.<Object>any(),
        Matchers.<Object>any())).
        thenReturn(counterRow1).
        thenReturn(null);
    when(countersMap.keySet()).thenReturn(hs);

    assertTrue(handler.queueIncrementColumnValues(incrs));

    c.call();
    verify(htm).incrementColumnValue(
        row1.array(), family1, qual1, 1);

    assertEquals(1, handler.getSuccessfulCoalescings());

  }
}
