package org.apache.hadoop.hbase.thrift;

import junit.framework.TestCase;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.thrift.generated.Increment;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.runner.RunWith;
import static org.mockito.Mockito.*;

import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;

import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest( { ThriftServer.HBaseHandler.class } )
public class TestThriftServerQueueICV extends TestCase {

  private ThreadPoolExecutor pool;
  private BlockingQueue queue;
  private ThriftServer.HBaseHandler handler;

  @Override
  public void setUp() throws Exception {
    disableJMX();

    // Common mockout remove HBA calls into noops basically.
    HBaseAdmin hbaseAdminMocked = mock(HBaseAdmin.class);
    PowerMockito.whenNew(HBaseAdmin.class).withArguments(any()).thenReturn(hbaseAdminMocked);

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
  }

  private void disableJMX() throws Exception {
    MBeanServer mbs = mock(MBeanServer.class);
    // Remove the JMX to prevent spurious log messages
    PowerMockito.mockStatic(ManagementFactory.class);
    when(ManagementFactory.getPlatformMBeanServer()).thenReturn(mbs);
    PowerMockito.whenNew(ObjectName.class)
        .withParameterTypes(String.class)
        .withArguments(any())
        .thenReturn(null);
  }


  public void testQueueSuccessful() throws Exception {
    when(queue.size())
        .thenReturn(0)
        .thenReturn(1000);

    List mList = mock(List.class);

    assertTrue(handler.queueIncrementColumnValues(mList));

    assertTrue(handler.queueIncrementColumnValues(mList));

    verify(pool, times(2)).submit(any(Callable.class));
  }
  
  public void testQueueFailure() throws Exception {
    when(queue.size()).thenReturn(1001)
        .thenReturn(2000);

    List mList = mock(List.class);
    assertFalse(handler.queueIncrementColumnValues(mList));
    assertFalse(handler.queueIncrementColumnValues(mList));

    assertEquals(2, handler.getFailedIncrements());
    verify(pool, never()).submit(any(Callable.class));
  }

  public void testMixedSuccessFail() throws Exception {
    when(queue.size()).thenReturn(1)
        .thenReturn(2000)
        .thenReturn(20);

    List mList = mock(List.class);
    assertTrue(handler.queueIncrementColumnValues(mList));
    assertFalse(handler.queueIncrementColumnValues(mList));
    assertTrue(handler.queueIncrementColumnValues(mList));

    verify(pool, times(2)).submit(any(Callable.class));
  }

  /**
   * Create and capture the callables, then "run" them and verify they
   * do the right thing.
   * @throws Exception
   */
  private static final byte[] tableA = Bytes.toBytes("tableA");
  private static final byte[] tableB = Bytes.toBytes("tableB");
  private static final byte[] row1 = Bytes.toBytes("row1");
  private static final byte[] row2 = Bytes.toBytes("row2");

  private static final byte[] family1 = Bytes.toBytes("family1");
  private static final byte[] qual1 = Bytes.toBytes("qual1");

  private static final byte[] family2 = Bytes.toBytes("family2");
  private static final byte[] qual2 = Bytes.toBytes("qual2");

  private static final byte[] colon = Bytes.toBytes(":");

  public void testCallablesDoRightThing() throws Exception {
    List<Increment> incrs = new ArrayList<Increment>();
    incrs.add(new Increment(tableA, row1, Bytes.add(family1, colon, qual1), 1));
    incrs.add(new Increment(tableB, row2, Bytes.add(family2, colon, qual2), 1000));

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
    verify(htm).incrementColumnValue(
        row1, family1, qual1, 1);
    verify(htm).incrementColumnValue(
        row2, family2, qual2, 1000);
  }

  public void testCallableErrorHandling() throws Exception {
    List<Increment> incrs = new ArrayList<Increment>();
    incrs.add(new Increment(tableA, row1, Bytes.add(family1, colon, qual1), 1));
    incrs.add(new Increment(tableA, row2, Bytes.add(family1, colon, qual1), 1));
    incrs.add(new Increment(tableB, row1, Bytes.add(family2, colon, qual2), 1000));
    incrs.add(new Increment(tableB, row2, Bytes.add(family2, colon, qual2), 1000));

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

    reset(htm);
    when(htm.incrementColumnValue(Matchers.<byte[]>any(),
        Matchers.<byte[]>any(),
        Matchers.<byte[]>any(),
        anyLong()))
        .thenReturn(0L)
        .thenThrow(new IOException());
    assertEquals(3, c.call());
  }
}
