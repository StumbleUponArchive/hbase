package org.apache.hadoop.hbase.thrift;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"org.w3c.*",
    "javax.xml.*",
    "javax.management.*",
    "com.sun.org.apache.xerces.*",
    "org.apache.log4j.*",
    "org.apache.commons.logging.*"})
@PrepareForTest( { ThriftServer.HBaseHandler.class, HBaseConfiguration.class } )
public class TestThriftServerQueueICV extends TestCase {

  private ThreadPoolExecutor pool;
  private BlockingQueue queue;
  private ThriftServer.HBaseHandler handler;
  private int failQueueSize;

  @Override
  public void setUp() throws Exception {

    // Common mockout remove HBA calls into noops basically.
    HBaseAdmin hbaseAdminMocked = mock(HBaseAdmin.class);
    PowerMockito.whenNew(HBaseAdmin.class).withArguments(any()).
        thenReturn(hbaseAdminMocked);
    // Somehow powermockito messes up with .xml files loading and this method
    // throws an exception
    PowerMockito.suppress(HBaseConfiguration.class.
        getDeclaredMethod("checkDefaultsVersion", Configuration.class));

    this.pool = mock(ThreadPoolExecutor.class);
    PowerMockito.whenNew(ThreadPoolExecutor.class).
        withParameterTypes(Integer.TYPE, Integer.TYPE,
            Long.TYPE, TimeUnit.class,
            BlockingQueue.class, ThreadFactory.class )
        .withArguments(any(), any(), any(), any(), any(), any())
        .thenReturn(pool);


    // Create a sub-mock.
    this.queue = mock(BlockingQueue.class);
    when(pool.getQueue()).thenReturn(this.queue);

    handler = new ThriftServer.HBaseHandler();
    failQueueSize = handler.getFailQueueSize();
  }

  public void testQueueSuccessful() throws Exception {
    when(queue.size())
        .thenReturn(0)
        .thenReturn(failQueueSize);

    List<Increment> list = new ArrayList<Increment>();

    assertTrue(handler.queueIncrementColumnValues(list));

    assertTrue(handler.queueIncrementColumnValues(list));

    verify(pool, times(2)).submit(any(Callable.class));
  }

  public void testQueueFailure() throws Exception {
    when(queue.size()).thenReturn(failQueueSize+1)
        .thenReturn(failQueueSize+1000);

    List<Increment> list = new ArrayList<Increment>();
    assertFalse(handler.queueIncrementColumnValues(list));
    assertFalse(handler.queueIncrementColumnValues(list));

    assertEquals(2, handler.getFailedIncrements());
    verify(pool, never()).submit(any(Callable.class));
  }

  public void testMixedSuccessFail() throws Exception {
    when(queue.size()).thenReturn(1)
        .thenReturn(failQueueSize+1000)
        .thenReturn(20);

    List<Increment> list = new ArrayList<Increment>();
    assertTrue(handler.queueIncrementColumnValues(list));
    assertFalse(handler.queueIncrementColumnValues(list));
    assertTrue(handler.queueIncrementColumnValues(list));

    verify(pool, times(2)).submit(any(Callable.class));
  }

  /**
   * Create and capture the callables, then "run" them and verify they
   * do the right thing.
   * @throws Exception
   */
  private static ByteBuffer $bb(String s) {
    return ByteBuffer.wrap(Bytes.toBytes(s));
  }
  private static final ByteBuffer tableA = $bb("tableA");
  private static final ByteBuffer tableB = $bb("tableB");
  private static final ByteBuffer row1 = $bb("row1");
  private static final ByteBuffer row2 = $bb("row2");

  private static final byte[] family1 = Bytes.toBytes("family1");
  private static final byte[] qual1 = Bytes.toBytes("qual1");

  private static final byte[] family2 = Bytes.toBytes("family2");
  private static final byte[] qual2 = Bytes.toBytes("qual2");

  private static final byte[] colon = Bytes.toBytes(":");

  public void testCallablesDoRightThing() throws Exception {
    List<Increment> incrs = new ArrayList<Increment>();
    incrs.add(new Increment(tableA, row1, ByteBuffer.wrap(Bytes.add(family1, colon, qual1)), 1));
    incrs.add(new Increment(tableB, row2, ByteBuffer.wrap(Bytes.add(family2, colon, qual2)), 1000));

    // allow the queued things to proceed.
    when(queue.size()).thenReturn(1);

    // make the handler a spy so we can inject our own special HTable.
    handler = spy(handler);
    assertTrue(handler.queueIncrementColumnValues(incrs));

    // what follows is lovely complex. First we capture the argument for the pool
    // now we have access to the callable which uses an inner class to call
    // getTable() on handler and THEN calls HTable API calls which we verify below.

    ArgumentCaptor<Callable> captor = ArgumentCaptor.forClass(Callable.class);
    verify(pool).submit(captor.capture());
    Callable c = captor.getValue();

    HTable htm = mock(HTable.class);
    doReturn(htm).when(handler).getTable(Matchers.<byte[]>any());

    c.call(); // this runs "live" code

    // verify on the htm (htable mock).
    // row1.array() is a hax but i know that the backing array is sized right.
    verify(htm).incrementColumnValue(
        row1.array(), family1, qual1, 1);
    verify(htm).incrementColumnValue(
        row2.array(), family2, qual2, 1000);
  }

  public void testCallableErrorHandling() throws Exception {
    List<Increment> incrs = new ArrayList<Increment>();
    incrs.add(new Increment(tableA, row1, ByteBuffer.wrap(Bytes.add(family1, colon, qual1)), 1));
    incrs.add(new Increment(tableA, row2, ByteBuffer.wrap(Bytes.add(family1, colon, qual1)), 1));
    incrs.add(new Increment(tableB, row1, ByteBuffer.wrap(Bytes.add(family2, colon, qual2)), 1000));
    incrs.add(new Increment(tableB, row2, ByteBuffer.wrap(Bytes.add(family2, colon, qual2)), 1000));

    when(queue.size()).thenReturn(1);
    handler = spy(handler);
    assertTrue(handler.queueIncrementColumnValues(incrs));

    ArgumentCaptor<Callable> captor = ArgumentCaptor.forClass(Callable.class);
    verify(pool).submit(captor.capture());
    Callable c = captor.getValue();

    HTable htm = mock(HTable.class);
    doReturn(htm).when(handler).getTable(Matchers.<byte[]>any());

    // fail everything.
    when(htm.incrementColumnValue(Matchers.<byte[]>any(),
        Matchers.<byte[]>any(),
        Matchers.<byte[]>any(),
        anyLong()))
        .thenThrow(new IOException());

    // the callable returns the # of failures we encountered
    assertEquals(4, c.call());
  }
}
