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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.Reference.Range;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Executes region split as a "transaction".  Call {@link #prepare()} to setup
 * the transaction, {@link #execute(OnlineRegions)} to run the transaction and
 * {@link #rollback(OnlineRegions)} to cleanup if execute fails.
 *
 * <p>Here is an example of how you would use this class:
 * <pre>
 *  SplitTransaction st = new SplitTransaction(this.conf, parent, midKey)
 *  if (!st.prepare()) return;
 *  try {
 *    st.execute(myOnlineRegions);
 *  } catch (IOException ioe) {
 *    try {
 *      st.rollback(myOnlineRegions);
 *      return;
 *    } catch (RuntimeException e) {
 *      myAbortable.abort("Failed split, abort");
 *    }
 *  }
 * </Pre>
 * <p>This class is not thread safe.  Caller needs ensure split is run by
 * one thread only.
 */
class SplitTransaction {
  private static final Log LOG = LogFactory.getLog(SplitTransaction.class);
  private static final String SPLITDIR = "splits";

  /*
   * Region to split
   */
  private final HRegion parent;
  private HRegionInfo hri_a;
  private HRegionInfo hri_b;
  private Path splitdir;
  private long fileSplitTimeout = 30000;

  /*
   * Row to split around
   */
  private final byte [] splitrow;

  /**
   * Types to add to the transaction journal
   */
  enum JournalEntry {
    /**
     * We created the temporary split data directory.
     */
    CREATE_SPLIT_DIR,
    /**
     * Closed the parent region.
     */
    CLOSED_PARENT_REGION,
    /**
     * The parent has been taken out of the server's online regions list.
     */
    OFFLINED_PARENT,
    /**
     * Started in on creation of the first daughter region.
     */
    STARTED_REGION_A_CREATION,
    /**
     * Started in on the creation of the second daughter region.
     */
    STARTED_REGION_B_CREATION
  }

  /*
   * Journal of how far the split transaction has progressed.
   */
  private final List<JournalEntry> journal = new ArrayList<JournalEntry>();

  /**
   * Constructor
   * @param c Configuration to use running split
   * @param r Region to split
   * @param splitrow Row to split around
   */
  SplitTransaction(final HRegion r, final byte [] splitrow) {
    this.parent = r;
    this.splitrow = splitrow;
    this.splitdir = getSplitDir(this.parent);
  }

  /**
   * Does checks on split inputs.
   * @return <code>true</code> if the region is splittable else
   * <code>false</code> if it is not (e.g. its already closed, etc.).
   */
  public boolean prepare() {
    if (this.parent.isClosed() || this.parent.isClosing()) return false;
    HRegionInfo hri = this.parent.getRegionInfo();
    // Check splitrow.
    byte [] startKey = hri.getStartKey();
    byte [] endKey = hri.getEndKey();
    if (Bytes.equals(startKey, splitrow) ||
        !this.parent.getRegionInfo().containsRow(splitrow)) {
      LOG.info("Split row is not inside region key range or is equal to " +
          "startkey: " + Bytes.toString(this.splitrow));
      return false;
    }
    long rid = getDaughterRegionIdTimestamp(hri);
    this.hri_a = new HRegionInfo(hri.getTableDesc(), startKey, this.splitrow,
      false, rid);
    this.hri_b = new HRegionInfo(hri.getTableDesc(), this.splitrow, endKey,
      false, rid);
    return true;
  }

  /**
   * Calculate daughter regionid to use.
   * @param hri Parent {@link HRegionInfo}
   * @return Daughter region id (timestamp) to use.
   */
  private static long getDaughterRegionIdTimestamp(final HRegionInfo hri) {
    long rid = EnvironmentEdgeManager.currentTimeMillis();
    // Regionid is timestamp.  Can't be less than that of parent else will insert
    // at wrong location in .META. (See HBASE-710).
    if (rid < hri.getRegionId()) {
      LOG.warn("Clock skew; parent regions id is " + hri.getRegionId() +
        " but current time here is " + rid);
      rid = hri.getRegionId() + 1;
    }
    return rid;
  }

  /**
   * Run the transaction.
   * @param or Object that can online/offline parent region.
   * @throws IOException If thrown, transaction failed. Call {@link #rollback(OnlineRegions)}
   * @return Regions created
   * @see #rollback(OnlineRegions)
   */
  public PairOfSameType<HRegion> execute(final OnlineRegions or) throws IOException {
    return execute(or, or != null);
  }

  /**
   * Run the transaction.
   * @param or Object that can online/offline parent region.  Can be null (Tests
   * will pass null).
   * @param If <code>true</code>, update meta (set to false when testing).
   * @throws IOException If thrown, transaction failed. Call {@link #rollback(OnlineRegions)}
   * @return Regions created
   * @see #rollback(OnlineRegions)
   */
  PairOfSameType<HRegion> execute(final OnlineRegions or, final boolean updateMeta)
  throws IOException {
    LOG.info("Starting split of region " + this.parent);

    // We'll need one of these later but get it now because if we fail there
    // is nothing to undo.
    HTable t = null;
    if (updateMeta) t = getTable(this.parent.getConf());

    createSplitDir(this.parent.getFilesystem(), this.splitdir);
    this.journal.add(JournalEntry.CREATE_SPLIT_DIR);

    List<StoreFile> hstoreFilesToSplit = this.parent.close(false);
    this.journal.add(JournalEntry.CLOSED_PARENT_REGION);

    if (or != null) or.removeFromOnlineRegions(this.parent.getRegionInfo());
    this.journal.add(JournalEntry.OFFLINED_PARENT);

    splitStoreFiles(this.splitdir, hstoreFilesToSplit);
    // splitStoreFiles creates daughter region dirs under the parent splits dir
    // Nothing to unroll here if failure -- clean up of CREATE_SPLIT_DIR will
    // clean this up.

    // Log to the journal that we are creating region A, the first daughter
    // region.  We could fail halfway through.  If we do, we could have left
    // stuff in fs that needs cleanup -- a storefile or two.  Thats why we
    // add entry to journal BEFORE rather than AFTER the change.
    this.journal.add(JournalEntry.STARTED_REGION_A_CREATION);
    HRegion a = createDaughterRegion(this.hri_a);

    // Ditto
    this.journal.add(JournalEntry.STARTED_REGION_B_CREATION);
    HRegion b = createDaughterRegion(this.hri_b);

    Put editParentPut = createOfflineParentPut();
    if (t != null) t.put(editParentPut);

    // The is the point of no return.  We are committed to the split now.  Up to
    // a failure editing parent in meta or a crash of the hosting regionserver,
    // we could rollback (or, if crash, we could cleanup on redeploy) but now
    // meta has been changed, we can only go forward.  If the below last steps
    // do not complete, repair has to be done by another agent.  For example,
    // basescanner, at least up till master rewrite, would add daughter rows if
    // missing from meta.  It could do this because the parent edit includes the
    // daughter specs.  In Bigtable paper, they have another mechanism where
    // some feedback to the master somehow flags it that split is incomplete and
    // needs fixup.  Whatever the mechanism, its a TODO that we have some fixup.
    
    // I looked at writing the put of the parent edit above out to the WAL log
    // before changing meta with the notion that should we fail, then on replay
    // the offlining of the parent and addition of daughters up into meta could
    // be reinserted.  The edits would have to be 'special' and given how our
    // splits work, splitting by region, I think the replay would have to happen
    // inside in the split code -- as soon as it saw one of these special edits,
    // rather than write the edit out a file for the .META. region to replay or
    // somehow, write it out to this regions edits file for it to handle on
    // redeploy -- this'd be whacky, we'd be telling meta about a split during
    // the deploy of the parent -- instead we'd have to play the edit inside
    // in the split code somehow; this would involve a stop-the-splitting till
    // meta had been edited which might hold up splitting a good while.

    // Finish up the meta edits.  If these fail, another agent needs to do fixup
    HRegionInfo hri = this.hri_a;
    try {
      if (t != null) t.put(createDaughterPut(hri));
      hri = this.hri_b;
      if (t != null) t.put(createDaughterPut(hri));
    } catch (IOException e) {
      // Don't let this out or we'll run rollback.
      LOG.warn("Failed adding daughter " + hri.toString());
    }
    // This should not fail because the HTable instance we are using is not
    // running a buffer -- its immediately flushing its puts.
    if (t != null) t.close();

    // Leaving here, the splitdir with its dross will be in place but since the
    // split was successful, just leave it; it'll be cleaned when parent is
    // deleted and cleaned up.
    return new PairOfSameType<HRegion>(a, b);
  }

  private static Path getSplitDir(final HRegion r) {
    return new Path(r.getRegionDir(), SPLITDIR);
  }

  /**
   * @param fs Filesystem to use
   * @param splitdir Directory to store temporary split data in
   * @throws IOException If <code>splitdir</code> already exists or we fail
   * to create it.
   * @see #cleanupSplitDir(FileSystem, Path)
   */
  private static void createSplitDir(final FileSystem fs, final Path splitdir)
  throws IOException {
    if (fs.exists(splitdir)) throw new IOException("Splitdir already exits? " + splitdir);
    if (!fs.mkdirs(splitdir)) throw new IOException("Failed create of " + splitdir);
  }

  private static void cleanupSplitDir(final FileSystem fs, final Path splitdir)
  throws IOException {
    // Splitdir may have been cleaned up by reopen of the parent dir.
    deleteDir(fs, splitdir, false);
  }

  /**
   * @param fs Filesystem to use
   * @param dir Directory to delete
   * @param mustPreExist If true, we'll throw exception if <code>dir</code>
   * does not preexist, else we'll just pass.
   * @throws IOException Thrown if we fail to delete passed <code>dir</code>
   */
  private static void deleteDir(final FileSystem fs, final Path dir,
      final boolean mustPreExist)
  throws IOException {
    if (!fs.exists(dir)) {
      if (mustPreExist) throw new IOException(dir.toString() + " does not exist!");
    } else if (!fs.delete(dir, true)) {
      throw new IOException("Failed delete of " + dir);
    }
  }

  private void splitStoreFiles(final Path splitdir,
    final List<StoreFile> hstoreFilesToSplit)
  throws IOException {
    if (hstoreFilesToSplit == null) {
      // Could be null because close didn't succeed -- for now consider it fatal
      throw new IOException("Close returned empty list of StoreFiles");
    }
    // The following code sets up a thread pool executor with as many slots as
    // there's files to split. It then fires up everything, waits for
    // completion and finally checks for any exception
    int nbFiles = hstoreFilesToSplit.size();
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setNameFormat("StoreFileSplitter-%1$d");
    ThreadFactory factory = builder.build();
    ThreadPoolExecutor threadPool =
      (ThreadPoolExecutor) Executors.newFixedThreadPool(nbFiles, factory);
    List<Future<Void>> futures = new ArrayList<Future<Void>>(nbFiles);

     // Split each store file.
    for (StoreFile sf: hstoreFilesToSplit) {
      //splitStoreFile(sf, splitdir);
      StoreFileSplitter sfs = new StoreFileSplitter(sf, splitdir);
      futures.add(threadPool.submit(sfs));
    }
    // Shutdown the pool
    threadPool.shutdown();

    // Wait for all the tasks to finish
    try {
      boolean stillRunning = !threadPool.awaitTermination(
          this.fileSplitTimeout, TimeUnit.MILLISECONDS);
      if (stillRunning) {
        threadPool.shutdownNow();
        throw new IOException("Took too long to split the" +
            " files and create the references, aborting split");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for file splitters", e);
    }

    // Look for any exception
    for (Future future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            "Interrupted while trying to get the results of file splitters",e );
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      }
    }
  }

  private void splitStoreFile(final StoreFile sf, final Path splitdir)
  throws IOException {
    FileSystem fs = this.parent.getFilesystem();
    byte [] family = sf.getFamily();
    String encoded = this.hri_a.getEncodedName();
    Path storedir = Store.getStoreHomedir(splitdir, encoded, family);
    StoreFile.split(fs, storedir, sf, this.splitrow, Range.bottom);
    encoded = this.hri_b.getEncodedName();
    storedir = Store.getStoreHomedir(splitdir, encoded, family);
    StoreFile.split(fs, storedir, sf, this.splitrow, Range.top);
  }

  /**
   * Utility class used to do the file splitting / reference writing
   * in parallel instead of sequentially.
   */
  class StoreFileSplitter implements Callable<Void> {

    private final StoreFile sf;
    private final Path splitdir;

    /**
     * Constructor that takes what it needs to split
     * @param sf which file
     * @param splitdir where the splitting is done
     */
    public StoreFileSplitter(final StoreFile sf, final Path splitdir) {
      this.sf = sf;
      this.splitdir = splitdir;
    }

    public Void call() throws IOException {
      splitStoreFile(sf, splitdir);
      return null;
    }
  }

  /**
   * @param hri
   * @return Created daughter HRegion.
   * @throws IOException
   * @see #cleanupDaughterRegion(FileSystem, Path, HRegionInfo)
   */
  HRegion createDaughterRegion(final HRegionInfo hri)
  throws IOException {
    // Package private so unit tests have access.
    FileSystem fs = this.parent.getFilesystem();
    Path regionDir = getSplitDirForDaughter(this.parent.getFilesystem(),
      this.splitdir, hri);
    HRegion r = HRegion.newHRegion(this.parent.getTableDir(),
      this.parent.getLog(), fs, this.parent.getConf(),
      hri, null);
    HRegion.moveInitialFilesIntoPlace(fs, regionDir, r.getRegionDir());
    return r;
  }

  private static void cleanupDaughterRegion(final FileSystem fs,
    final Path tabledir, final String encodedName)
  throws IOException {
    Path regiondir = HRegion.getRegionDir(tabledir, encodedName);
    // Dir may not preexist.
    deleteDir(fs, regiondir, false);
  }

  /*
   * Get the daughter directories in the splits dir.  The splits dir is under
   * the parent regions' directory.
   * @param fs
   * @param splitdir
   * @param hri
   * @return Path to daughter split dir.
   * @throws IOException
   */
  private static Path getSplitDirForDaughter(final FileSystem fs,
      final Path splitdir, final HRegionInfo hri)
  throws IOException {
    return new Path(splitdir, hri.getEncodedName());
  }

  /*
   * @param r Parent region we want to edit.
   * @return An HTable instance against the meta table that holds passed
   * <code>r</code>; it has autoFlush enabled so we immediately send puts (No
   * buffering enabled).
   * @throws IOException
   */
  private HTable getTable(final Configuration conf) throws IOException {
    // When a region is split, the META table needs to updated if we're
    // splitting a 'normal' region, and the ROOT table needs to be
    // updated if we are splitting a META region.
    HTable t = null;
    if (this.parent.getRegionInfo().isMetaTable()) {
      t = new HTable(conf, HConstants.ROOT_TABLE_NAME);
    } else {
      t = new HTable(conf, HConstants.META_TABLE_NAME);
    }
    // Flush puts as we send them -- no buffering.
    t.setAutoFlush(true);
    return t;
  }


  private Put createOfflineParentPut() throws IOException  {
    HRegionInfo editedParentRegionInfo =
      new HRegionInfo(this.parent.getRegionInfo());
    editedParentRegionInfo.setOffline(true);
    editedParentRegionInfo.setSplit(true);
    Put put = new Put(editedParentRegionInfo.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
      Writables.getBytes(editedParentRegionInfo));
    put.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
        HConstants.EMPTY_BYTE_ARRAY);
    put.add(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
        HConstants.EMPTY_BYTE_ARRAY);
    put.add(HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER,
      Writables.getBytes(this.hri_a));
    put.add(HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER,
      Writables.getBytes(this.hri_b));
    return put;
  }

  private Put createDaughterPut(final HRegionInfo daughter)
  throws IOException {
    Put p = new Put(daughter.getRegionName());
    p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
      Writables.getBytes(daughter));
    return p;
  }

  /**
   * @param or Object that can online/offline parent region.  Can be passed null
   * by unit tests.
   * @return The region we were splitting
   * @throws IOException If thrown, rollback failed.  Take drastic action.
   */
  public void rollback(final OnlineRegions or) throws IOException {
    FileSystem fs = this.parent.getFilesystem();
    ListIterator<JournalEntry> iterator =
      this.journal.listIterator(this.journal.size());
    while (iterator.hasPrevious()) {
      JournalEntry je = iterator.previous();
      switch(je) {
      case CREATE_SPLIT_DIR:
        cleanupSplitDir(fs, this.splitdir);
        break;

      case CLOSED_PARENT_REGION:
        // So, this returns a seqid but if we just closed and then reopened, we
        // should be ok. On close, we flushed using sequenceid obtained from
        // hosting regionserver so no need to propagate the sequenceid returned
        // out of initialize below up into regionserver as we normally do.
        // TODO: Verify.
        this.parent.initialize();
        break;

      case STARTED_REGION_A_CREATION:
        cleanupDaughterRegion(fs, this.parent.getTableDir(),
          this.hri_a.getEncodedName());
        break;

      case STARTED_REGION_B_CREATION:
        cleanupDaughterRegion(fs, this.parent.getTableDir(),
          this.hri_b.getEncodedName());
        break;

      case OFFLINED_PARENT:
        if (or != null) or.addToOnlineRegions(this.parent);
        break;

      default:
        throw new RuntimeException("Unhandled journal entry: " + je);
      }
    }
  }

  HRegionInfo getFirstDaughter() {
    return hri_a;
  }

  HRegionInfo getSecondDaughter() {
    return hri_b;
  }

  // For unit testing.
  Path getSplitDir() {
    return this.splitdir;
  }

  /**
   * Clean up any split detritus that may have been left around from previous
   * split attempts.
   * Call this method on initial region deploy.  Cleans up any mess
   * left by previous deploys of passed <code>r</code> region.
   * @param r
   * @throws IOException 
   */
  static void cleanupAnySplitDetritus(final HRegion r) throws IOException {
    Path splitdir = getSplitDir(r);
    FileSystem fs = r.getFilesystem();
    if (!fs.exists(splitdir)) return;
    // Look at the splitdir.  It could have the encoded names of the daughter
    // regions we tried to make.  See if the daughter regions actually got made
    // out under the tabledir.  If here under splitdir still, then the split did
    // not complete.  Try and do cleanup.  This code WILL NOT catch the case
    // where we successfully created daughter a but regionserver crashed during
    // the creation of region b.  In this case, there'll be an orphan daughter
    // dir in the filesystem.  TOOD: Fix.
    FileStatus [] daughters = fs.listStatus(splitdir, new FSUtils.DirFilter(fs));
    for (int i = 0; i < daughters.length; i++) {
      cleanupDaughterRegion(fs, r.getTableDir(),
        daughters[i].getPath().getName());
    }
    cleanupSplitDir(r.getFilesystem(), splitdir);
    LOG.info("Cleaned up old failed split transaction detritus: " + splitdir);
  }
}