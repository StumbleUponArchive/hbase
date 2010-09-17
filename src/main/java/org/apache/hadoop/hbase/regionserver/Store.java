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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * A Store holds a column family in a Region.  Its a memstore and a set of zero
 * or more StoreFiles, which stretch backwards over time.
 *
 * <p>There's no reason to consider append-logging at this level; all logging
 * and locking is handled at the HRegion level.  Store just provides
 * services to manage sets of StoreFiles.  One of the most important of those
 * services is compaction services where files are aggregated once they pass
 * a configurable threshold.
 *
 * <p>The only thing having to do with logs that Store needs to deal with is
 * the reconstructionLog.  This is a segment of an HRegion's log that might
 * NOT be present upon startup.  If the param is NULL, there's nothing to do.
 * If the param is non-NULL, we need to process the log to reconstruct
 * a TreeMap that might not have been written to disk before the process
 * died.
 *
 * <p>It's assumed that after this constructor returns, the reconstructionLog
 * file will be deleted (by whoever has instantiated the Store).
 *
 * <p>Locking and transactions are handled at a higher level.  This API should
 * not be called directly but by an HRegion manager.
 */
public class Store implements HeapSize {
  static final Log LOG = LogFactory.getLog(Store.class);
  protected final MemStore memstore;
  // This stores directory in the filesystem.
  private final Path homedir;
  private final HRegion region;
  private final HColumnDescriptor family;
  final FileSystem fs;
  final Configuration conf;
  // ttl in milliseconds.
  protected long ttl;
  private long majorCompactionTime;
  private int maxFilesToCompact;
  private final long desiredMaxFileSize;
  private volatile long storeSize = 0L;
  private final Object flushLock = new Object();
  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final String storeNameStr;
  private final boolean inMemory;

  /*
   * List of store files inside this store. This is an immutable list that
   * is atomically replaced when its contents change.
   */
  private ImmutableList<StoreFile> storefiles = null;


  // All access must be synchronized.
  private final CopyOnWriteArraySet<ChangedReadersObserver> changedReaderObservers =
    new CopyOnWriteArraySet<ChangedReadersObserver>();

  private final Object compactLock = new Object();
  private final int compactionThreshold;
  private final int blocksize;
  private final boolean blockcache;
  private final Compression.Algorithm compression;

  // Comparing KeyValues
  final KeyValue.KVComparator comparator;

  /**
   * Constructor
   * @param basedir qualified path under which the region directory lives;
   * generally the table subdirectory
   * @param region
   * @param family HColumnDescriptor for this column
   * @param fs file system object
   * @param conf configuration object
   * failed.  Can be null.
   * @throws IOException
   */
  protected Store(Path basedir, HRegion region, HColumnDescriptor family,
    FileSystem fs, Configuration conf)
  throws IOException {
    HRegionInfo info = region.regionInfo;
    this.fs = fs;
    this.homedir = getStoreHomedir(basedir, info.getEncodedName(), family.getName());
    if (!this.fs.exists(this.homedir)) {
      if (!this.fs.mkdirs(this.homedir))
        throw new IOException("Failed create of: " + this.homedir.toString());
    }
    this.region = region;
    this.family = family;
    this.conf = conf;
    this.blockcache = family.isBlockCacheEnabled();
    this.blocksize = family.getBlocksize();
    this.compression = family.getCompression();
    this.comparator = info.getComparator();
    // getTimeToLive returns ttl in seconds.  Convert to milliseconds.
    this.ttl = family.getTimeToLive();
    if (ttl == HConstants.FOREVER) {
      // default is unlimited ttl.
      ttl = Long.MAX_VALUE;
    } else if (ttl == -1) {
      ttl = Long.MAX_VALUE;
    } else {
      // second -> ms adjust for user data
      this.ttl *= 1000;
    }
    this.memstore = new MemStore(this.comparator);
    this.storeNameStr = Bytes.toString(this.family.getName());

    // By default, we compact if an HStore has more than
    // MIN_COMMITS_FOR_COMPACTION map files
    this.compactionThreshold =
      conf.getInt("hbase.hstore.compactionThreshold", 3);

    // Check if this is in-memory store
    this.inMemory = family.isInMemory();

    // By default we split region if a file > HConstants.DEFAULT_MAX_FILE_SIZE.
    long maxFileSize = info.getTableDesc().getMaxFileSize();
    if (maxFileSize == HConstants.DEFAULT_MAX_FILE_SIZE) {
      maxFileSize = conf.getLong("hbase.hregion.max.filesize",
        HConstants.DEFAULT_MAX_FILE_SIZE);
    }
    this.desiredMaxFileSize = maxFileSize;

    this.majorCompactionTime =
      conf.getLong(HConstants.MAJOR_COMPACTION_PERIOD, 86400000);
    if (family.getValue(HConstants.MAJOR_COMPACTION_PERIOD) != null) {
      String strCompactionTime =
        family.getValue(HConstants.MAJOR_COMPACTION_PERIOD);
      this.majorCompactionTime = (new Long(strCompactionTime)).longValue();
    }

    this.maxFilesToCompact = conf.getInt("hbase.hstore.compaction.max", 10);
    this.storefiles = ImmutableList.copyOf(loadStoreFiles());
  }

  public HColumnDescriptor getFamily() {
    return this.family;
  }

  /**
   * @return The maximum sequence id in all store files.
   */
  long getMaxSequenceId() {
    return StoreFile.getMaxSequenceIdInList(this.getStorefiles());
  }

  /**
   * @param tabledir
   * @param encodedName Encoded region name.
   * @param family
   * @return Path to family/Store home directory.
   */
  public static Path getStoreHomedir(final Path tabledir,
      final String encodedName, final byte [] family) {
    return new Path(tabledir, new Path(encodedName,
      new Path(Bytes.toString(family))));
  }

  /**
   * Return the directory in which this store stores its
   * StoreFiles
   */
  public Path getHomedir() {
    return homedir;
  }

  /*
   * Creates a series of StoreFile loaded from the given directory.
   * @throws IOException
   */
  private List<StoreFile> loadStoreFiles()
  throws IOException {
    ArrayList<StoreFile> results = new ArrayList<StoreFile>();
    FileStatus files[] = this.fs.listStatus(this.homedir);
    for (int i = 0; files != null && i < files.length; i++) {
      // Skip directories.
      if (files[i].isDir()) {
        continue;
      }
      Path p = files[i].getPath();
      // Check for empty file.  Should never be the case but can happen
      // after data loss in hdfs for whatever reason (upgrade, etc.): HBASE-646
      if (this.fs.getFileStatus(p).getLen() <= 0) {
        LOG.warn("Skipping " + p + " because its empty. HBASE-646 DATA LOSS?");
        continue;
      }
      StoreFile curfile = null;
      try {
        curfile = new StoreFile(fs, p, blockcache, this.conf,
            this.family.getBloomFilterType(), this.inMemory);
        curfile.createReader();
      } catch (IOException ioe) {
        LOG.warn("Failed open of " + p + "; presumption is that file was " +
          "corrupted at flush and lost edits picked up by commit log replay. " +
          "Verify!", ioe);
        continue;
      }
      long length = curfile.getReader().length();
      this.storeSize += length;
      if (LOG.isDebugEnabled()) {
        LOG.debug("loaded " + curfile.toStringDetailed());
      }
      results.add(curfile);
    }
    Collections.sort(results, StoreFile.Comparators.FLUSH_TIME);
    return results;
  }

  /**
   * Adds a value to the memstore
   *
   * @param kv
   * @return memstore size delta
   */
  protected long add(final KeyValue kv) {
    lock.readLock().lock();
    try {
      return this.memstore.add(kv);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Adds a value to the memstore
   *
   * @param kv
   * @return memstore size delta
   */
  protected long delete(final KeyValue kv) {
    lock.readLock().lock();
    try {
      return this.memstore.delete(kv);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @return All store files.
   */
  List<StoreFile> getStorefiles() {
    return this.storefiles;
  }

  public void bulkLoadHFile(String srcPathStr) throws IOException {
    Path srcPath = new Path(srcPathStr);

    HFile.Reader reader  = null;
    try {
      LOG.info("Validating hfile at " + srcPath + " for inclusion in "
          + "store " + this + " region " + this.region);
      reader = new HFile.Reader(srcPath.getFileSystem(conf),
          srcPath, null, false);
      reader.loadFileInfo();

      byte[] firstKey = reader.getFirstRowKey();
      byte[] lk = reader.getLastKey();
      byte[] lastKey =
          (lk == null) ? null :
              KeyValue.createKeyValueFromKey(lk).getRow();

      LOG.debug("HFile bounds: first=" + Bytes.toStringBinary(firstKey) +
          " last=" + Bytes.toStringBinary(lastKey));
      LOG.debug("Region bounds: first=" +
          Bytes.toStringBinary(region.getStartKey()) +
          " last=" + Bytes.toStringBinary(region.getEndKey()));

      HRegionInfo hri = region.getRegionInfo();
      if (!hri.containsRange(firstKey, lastKey)) {
        throw new WrongRegionException(
            "Bulk load file " + srcPathStr + " does not fit inside region "
            + this.region);
      }
    } finally {
      if (reader != null) reader.close();
    }

    // Move the file if it's on another filesystem
    FileSystem srcFs = srcPath.getFileSystem(conf);
    if (!srcFs.equals(fs)) {
      LOG.info("File " + srcPath + " on different filesystem than " +
          "destination store - moving to this filesystem.");
      Path tmpPath = getTmpPath();
      FileUtil.copy(srcFs, srcPath, fs, tmpPath, false, conf);
      LOG.info("Copied to temporary path on dst filesystem: " + tmpPath);
      srcPath = tmpPath;
    }

    Path dstPath = StoreFile.getRandomFilename(fs, homedir);
    LOG.info("Renaming bulk load file " + srcPath + " to " + dstPath);
    StoreFile.rename(fs, srcPath, dstPath);

    StoreFile sf = new StoreFile(fs, dstPath, blockcache,
        this.conf, this.family.getBloomFilterType(), this.inMemory);
    sf.createReader();

    LOG.info("Moved hfile " + srcPath + " into store directory " +
        homedir + " - updating store file list.");

    // Append the new storefile into the list
    this.lock.writeLock().lock();
    try {
      ArrayList<StoreFile> newFiles = new ArrayList<StoreFile>(storefiles);
      newFiles.add(sf);
      this.storefiles = ImmutableList.copyOf(newFiles);
      notifyChangedReadersObservers();
    } finally {
      this.lock.writeLock().unlock();
    }
    LOG.info("Successfully loaded store file " + srcPath
        + " into store " + this + " (new location: " + dstPath + ")");
  }

  /**
   * Get a temporary path in this region. These temporary files
   * will get cleaned up when the region is re-opened if they are
   * still around.
   */
  private Path getTmpPath() throws IOException {
    return StoreFile.getRandomFilename(
        fs, region.getTmpDir());
  }

  /**
   * Close all the readers
   *
   * We don't need to worry about subsequent requests because the HRegion holds
   * a write lock that will prevent any more reads or writes.
   *
   * @throws IOException
   */
  ImmutableList<StoreFile> close() throws IOException {
    this.lock.writeLock().lock();
    try {
      ImmutableList<StoreFile> result = storefiles;

      // Clear so metrics doesn't find them.
      storefiles = ImmutableList.of();

      for (StoreFile f: result) {
        f.closeReader();
      }
      LOG.debug("closed " + this.storeNameStr);
      return result;
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * Snapshot this stores memstore.  Call before running
   * {@link #flushCache(long, SortedSet<KeyValue>)} so it has some work to do.
   */
  void snapshot() {
    this.memstore.snapshot();
  }

  /**
   * Write out current snapshot.  Presumes {@link #snapshot()} has been called
   * previously.
   * @param logCacheFlushId flush sequence number
   * @param snapshot
   * @return true if a compaction is needed
   * @throws IOException
   */
  private StoreFile flushCache(final long logCacheFlushId,
      SortedSet<KeyValue> snapshot,
      TimeRangeTracker snapshotTimeRangeTracker) throws IOException {
    // If an exception happens flushing, we let it out without clearing
    // the memstore snapshot.  The old snapshot will be returned when we say
    // 'snapshot', the next time flush comes around.
    return internalFlushCache(snapshot, logCacheFlushId, snapshotTimeRangeTracker);
  }

  /*
   * @param cache
   * @param logCacheFlushId
   * @return StoreFile created.
   * @throws IOException
   */
  private StoreFile internalFlushCache(final SortedSet<KeyValue> set,
      final long logCacheFlushId,
      TimeRangeTracker snapshotTimeRangeTracker)
      throws IOException {
    StoreFile.Writer writer = null;
    long flushed = 0;
    // Don't flush if there are no entries.
    if (set.size() == 0) {
      return null;
    }
    long oldestTimestamp = System.currentTimeMillis() - ttl;
    // TODO:  We can fail in the below block before we complete adding this
    // flush to list of store files.  Add cleanup of anything put on filesystem
    // if we fail.
    synchronized (flushLock) {
      // A. Write the map out to the disk
      writer = createWriterInTmp(set.size());
      writer.setTimeRangeTracker(snapshotTimeRangeTracker);
      int entries = 0;
      try {
        for (KeyValue kv: set) {
          if (!isExpired(kv, oldestTimestamp)) {
            writer.append(kv);
            entries++;
            flushed += this.memstore.heapSizeChange(kv, true);
          }
        }
      } finally {
        // Write out the log sequence number that corresponds to this output
        // hfile.  The hfile is current up to and including logCacheFlushId.
        writer.appendMetadata(logCacheFlushId, false);
        writer.close();
      }
    }

    // Write-out finished successfully, move into the right spot
    Path dstPath = StoreFile.getUniqueFile(fs, homedir);
    LOG.info("Renaming flushed file at " + writer.getPath() + " to " + dstPath);
    fs.rename(writer.getPath(), dstPath);

    StoreFile sf = new StoreFile(this.fs, dstPath, blockcache,
      this.conf, this.family.getBloomFilterType(), this.inMemory);
    StoreFile.Reader r = sf.createReader();
    this.storeSize += r.length();
    if(LOG.isInfoEnabled()) {
      LOG.info("Added " + sf + ", entries=" + r.getEntries() +
        ", sequenceid=" + logCacheFlushId +
        ", memsize=" + StringUtils.humanReadableInt(flushed) +
        ", filesize=" + StringUtils.humanReadableInt(r.length()) +
        " to " + this.region.regionInfo.getRegionNameAsString());
    }
    return sf;
  }

  /*
   * @return Writer for a new StoreFile in the tmp dir.
   */
  private StoreFile.Writer createWriterInTmp(int maxKeyCount)
  throws IOException {
    return StoreFile.createWriter(this.fs, region.getTmpDir(), this.blocksize,
        this.compression, this.comparator, this.conf,
        this.family.getBloomFilterType(), maxKeyCount);
  }

  /*
   * Change storefiles adding into place the Reader produced by this new flush.
   * @param sf
   * @param set That was used to make the passed file <code>p</code>.
   * @throws IOException
   * @return Whether compaction is required.
   */
  private boolean updateStorefiles(final StoreFile sf,
                                   final SortedSet<KeyValue> set)
  throws IOException {
    this.lock.writeLock().lock();
    try {
      ArrayList<StoreFile> newList = new ArrayList<StoreFile>(storefiles);
      newList.add(sf);
      storefiles = ImmutableList.copyOf(newList);
      this.memstore.clearSnapshot(set);

      // Tell listeners of the change in readers.
      notifyChangedReadersObservers();

      return this.storefiles.size() >= this.compactionThreshold;
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /*
   * Notify all observers that set of Readers has changed.
   * @throws IOException
   */
  private void notifyChangedReadersObservers() throws IOException {
    for (ChangedReadersObserver o: this.changedReaderObservers) {
      o.updateReaders();
    }
  }

  /*
   * @param o Observer who wants to know about changes in set of Readers
   */
  void addChangedReaderObserver(ChangedReadersObserver o) {
    this.changedReaderObservers.add(o);
  }

  /*
   * @param o Observer no longer interested in changes in set of Readers.
   */
  void deleteChangedReaderObserver(ChangedReadersObserver o) {
    if (!this.changedReaderObservers.remove(o)) {
      LOG.warn("Not in set" + o);
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Compact the StoreFiles.  This method may take some time, so the calling
   * thread must be able to block for long periods.
   *
   * <p>During this time, the Store can work as usual, getting values from
   * StoreFiles and writing new StoreFiles from the memstore.
   *
   * Existing StoreFiles are not destroyed until the new compacted StoreFile is
   * completely written-out to disk.
   *
   * <p>The compactLock prevents multiple simultaneous compactions.
   * The structureLock prevents us from interfering with other write operations.
   *
   * <p>We don't want to hold the structureLock for the whole time, as a compact()
   * can be lengthy and we want to allow cache-flushes during this period.
   *
   * @param mc True to force a major compaction regardless of thresholds
   * @return row to split around if a split is needed, null otherwise
   * @throws IOException
   */
  StoreSize compact(final boolean mc) throws IOException {
    boolean forceSplit = this.region.shouldSplit(false);
    boolean majorcompaction = mc;
    synchronized (compactLock) {
      // filesToCompact are sorted oldest to newest.
      List<StoreFile> filesToCompact = this.storefiles;
      if (filesToCompact.isEmpty()) {
        LOG.debug(this.storeNameStr + ": no store files to compact");
        return null;
      }

      // Max-sequenceID is the last key of the storefiles TreeMap
      long maxId = StoreFile.getMaxSequenceIdInList(storefiles);

      // Check to see if we need to do a major compaction on this region.
      // If so, change doMajorCompaction to true to skip the incremental
      // compacting below. Only check if doMajorCompaction is not true.
      if (!majorcompaction) {
        majorcompaction = isMajorCompaction(filesToCompact);
      }

      boolean references = hasReferences(filesToCompact);
      if (!majorcompaction && !references &&
          (forceSplit || (filesToCompact.size() < compactionThreshold))) {
        return checkSplit(forceSplit);
      }

      // HBASE-745, preparing all store file sizes for incremental compacting
      // selection.
      int countOfFiles = filesToCompact.size();
      long totalSize = 0;
      long [] fileSizes = new long[countOfFiles];
      long skipped = 0;
      int point = 0;
      for (int i = 0; i < countOfFiles; i++) {
        StoreFile file = filesToCompact.get(i);
        Path path = file.getPath();
        if (path == null) {
          LOG.warn("Path is null for " + file);
          return null;
        }
        StoreFile.Reader r = file.getReader();
        if (r == null) {
          LOG.warn("StoreFile " + file + " has a null Reader");
          return null;
        }
        long len = file.getReader().length();
        fileSizes[i] = len;
        totalSize += len;
      }

      if (!majorcompaction && !references) {
        // Here we select files for incremental compaction.
        // The rule is: if the largest(oldest) one is more than twice the
        // size of the second, skip the largest, and continue to next...,
        // until we meet the compactionThreshold limit.

        // A problem with the above heuristic is that we could go through all of
        // filesToCompact and the above condition could hold for all files and
        // we'd end up with nothing to compact.  To protect against this, we'll
        // compact the tail -- up to the last 4 files -- of filesToCompact
        // regardless.
        // CHANGED FOR STUMBLEUPON, THE MINIMUM IS 2 SINCE 4 BASICALLY DISABLES MAJOR COMPACTIONS
        int tail = Math.min(countOfFiles, 2);
        for (point = 0; point < (countOfFiles - tail); point++) {
          if (((fileSizes[point] < fileSizes[point + 1] * 2) &&
               (countOfFiles - point) <= maxFilesToCompact)) {
            break;
          }
          skipped += fileSizes[point];
        }
        filesToCompact = new ArrayList<StoreFile>(filesToCompact.subList(point,
          countOfFiles));
        if (filesToCompact.size() <= 1) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipped compaction of 1 file; compaction size of " +
              this.storeNameStr + ": " +
              StringUtils.humanReadableInt(totalSize) + "; Skipped " + point +
              " files, size: " + skipped);
          }
          return checkSplit(forceSplit);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Compaction size of " + this.storeNameStr + ": " +
            StringUtils.humanReadableInt(totalSize) + "; Skipped " + point +
            " file(s), size: " + skipped);
        }
      }

      // Ready to go.  Have list of files to compact.
      LOG.info("Started compaction of " + filesToCompact.size() + " file(s) in " +
          this.storeNameStr + " of " + this.region.getRegionInfo().getRegionNameAsString() +
        (references? ", hasReferences=true,": " ") + " into " +
          region.getTmpDir() + ", sequenceid=" + maxId);
      StoreFile.Writer writer = compact(filesToCompact, majorcompaction, maxId);
      // Move the compaction into place.
      StoreFile sf = completeCompaction(filesToCompact, writer);
      if (LOG.isInfoEnabled()) {
        LOG.info("Completed" + (majorcompaction? " major ": " ") +
          "compaction of " + filesToCompact.size() + " file(s) in " +
          this.storeNameStr + " of " + this.region.getRegionInfo().getRegionNameAsString() +
          "; new storefile is " + (sf == null? "none": sf.toString()) +
          "; store size is " + StringUtils.humanReadableInt(storeSize));
      }
    }
    return checkSplit(forceSplit);
  }

  /*
   * @param files
   * @return True if any of the files in <code>files</code> are References.
   */
  private boolean hasReferences(Collection<StoreFile> files) {
    if (files != null && files.size() > 0) {
      for (StoreFile hsf: files) {
        if (hsf.isReference()) {
          return true;
        }
      }
    }
    return false;
  }

  /*
   * Gets lowest timestamp from files in a dir
   *
   * @param fs
   * @param dir
   * @throws IOException
   */
  private static long getLowestTimestamp(FileSystem fs, Path dir) throws IOException {
    FileStatus[] stats = fs.listStatus(dir);
    if (stats == null || stats.length == 0) {
      return 0l;
    }
    long lowTimestamp = Long.MAX_VALUE;
    for (int i = 0; i < stats.length; i++) {
      long timestamp = stats[i].getModificationTime();
      if (timestamp < lowTimestamp){
        lowTimestamp = timestamp;
      }
    }
    return lowTimestamp;
  }

  /*
   * @return True if we should run a major compaction.
   */
  boolean isMajorCompaction() throws IOException {
    return isMajorCompaction(storefiles);
  }

  /*
   * @param filesToCompact Files to compact. Can be null.
   * @return True if we should run a major compaction.
   */
  private boolean isMajorCompaction(final List<StoreFile> filesToCompact) throws IOException {
    boolean result = false;
    if (filesToCompact == null || filesToCompact.isEmpty() ||
        majorCompactionTime == 0) {
      return result;
    }
    long lowTimestamp = getLowestTimestamp(fs,
      filesToCompact.get(0).getPath().getParent());
    long now = System.currentTimeMillis();
    if (lowTimestamp > 0l && lowTimestamp < (now - this.majorCompactionTime)) {
      // Major compaction time has elapsed.
      long elapsedTime = now - lowTimestamp;
      if (filesToCompact.size() == 1 &&
          filesToCompact.get(0).isMajorCompaction() &&
          (this.ttl == HConstants.FOREVER || elapsedTime < this.ttl)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping major compaction of " + this.storeNameStr +
            " because one (major) compacted file only and elapsedTime " +
            elapsedTime + "ms is < ttl=" + this.ttl);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Major compaction triggered on store " + this.storeNameStr +
            "; time since last major compaction " + (now - lowTimestamp) + "ms");
        }
        result = true;
      }
    }
    return result;
  }

  /**
   * Do a minor/major compaction.  Uses the scan infrastructure to make it easy.
   *
   * @param filesToCompact which files to compact
   * @param majorCompaction true to major compact (prune all deletes, max versions, etc)
   * @param maxId Readers maximum sequence id.
   * @return Product of compaction or null if all cells expired or deleted and
   * nothing made it through the compaction.
   * @throws IOException
   */
  private StoreFile.Writer compact(final List<StoreFile> filesToCompact,
                               final boolean majorCompaction, final long maxId)
      throws IOException {
    // calculate maximum key count after compaction (for blooms)
    int maxKeyCount = 0;
    for (StoreFile file : filesToCompact) {
      StoreFile.Reader r = file.getReader();
      if (r != null) {
        // NOTE: getFilterEntries could cause under-sized blooms if the user
        //       switches bloom type (e.g. from ROW to ROWCOL)
        maxKeyCount += (r.getBloomFilterType() == family.getBloomFilterType())
          ? r.getFilterEntries() : r.getEntries();
      }
    }

    // For each file, obtain a scanner:
    List<StoreFileScanner> scanners = StoreFileScanner
      .getScannersForStoreFiles(filesToCompact, false, false);

    // Make the instantiation lazy in case compaction produces no product; i.e.
    // where all source cells are expired or deleted.
    StoreFile.Writer writer = null;
    try {
    if (majorCompaction) {
      InternalScanner scanner = null;
      try {
        Scan scan = new Scan();
        scan.setMaxVersions(family.getMaxVersions());
        scanner = new StoreScanner(this, scan, scanners);
        // since scanner.next() can return 'false' but still be delivering data,
        // we have to use a do/while loop.
        ArrayList<KeyValue> kvs = new ArrayList<KeyValue>();
        while (scanner.next(kvs)) {
          // output to writer:
          for (KeyValue kv : kvs) {
            if (writer == null) {
              writer = createWriterInTmp(maxKeyCount);
            }
            writer.append(kv);
          }
          kvs.clear();
        }
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
    } else {
      MinorCompactingStoreScanner scanner = null;
      try {
        scanner = new MinorCompactingStoreScanner(this, scanners);
        writer = createWriterInTmp(maxKeyCount);
        while (scanner.next(writer)) {
          // Nothing to do
        }
      } finally {
        if (scanner != null)
          scanner.close();
      }
    }
    } finally {
      if (writer != null) {
        writer.appendMetadata(maxId, majorCompaction);
        writer.close();
      }
    }
    return writer;
  }

  /*
   * It's assumed that the compactLock  will be acquired prior to calling this
   * method!  Otherwise, it is not thread-safe!
   *
   * <p>It works by processing a compaction that's been written to disk.
   *
   * <p>It is usually invoked at the end of a compaction, but might also be
   * invoked at HStore startup, if the prior execution died midway through.
   *
   * <p>Moving the compacted TreeMap into place means:
   * <pre>
   * 1) Moving the new compacted StoreFile into place
   * 2) Unload all replaced StoreFile, close and collect list to delete.
   * 3) Loading the new TreeMap.
   * 4) Compute new store size
   * </pre>
   *
   * @param compactedFiles list of files that were compacted
   * @param compactedFile StoreFile that is the result of the compaction
   * @return StoreFile created. May be null.
   * @throws IOException
   */
  private StoreFile completeCompaction(final List<StoreFile> compactedFiles,
                                       final StoreFile.Writer compactedFile)
      throws IOException {
    // 1. Moving the new files into place -- if there is a new file (may not
    // be if all cells were expired or deleted).
    StoreFile result = null;
    if (compactedFile != null) {
      Path p = null;
      try {
        p = StoreFile.rename(this.fs, compactedFile.getPath(),
          StoreFile.getRandomFilename(fs, this.homedir));
      } catch (IOException e) {
        LOG.error("Failed move of compacted file " + compactedFile.getPath(), e);
        return null;
      }
      result = new StoreFile(this.fs, p, blockcache, this.conf,
          this.family.getBloomFilterType(), this.inMemory);
      result.createReader();
    }
    this.lock.writeLock().lock();
    try {
      try {
        // 2. Unloading
        // 3. Loading the new TreeMap.
        // Change this.storefiles so it reflects new state but do not
        // delete old store files until we have sent out notification of
        // change in case old files are still being accessed by outstanding
        // scanners.
        ArrayList<StoreFile> newStoreFiles = new ArrayList<StoreFile>();
        for (StoreFile sf : storefiles) {
          if (!compactedFiles.contains(sf)) {
            newStoreFiles.add(sf);
          }
        }

        // If a StoreFile result, move it into place.  May be null.
        if (result != null) {
          newStoreFiles.add(result);
        }

	this.storefiles = ImmutableList.copyOf(newStoreFiles);

        // Tell observers that list of StoreFiles has changed.
        notifyChangedReadersObservers();
        // Finally, delete old store files.
        for (StoreFile hsf: compactedFiles) {
          hsf.deleteReader();
        }
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        LOG.error("Failed replacing compacted files in " + this.storeNameStr +
          ". Compacted file is " + (result == null? "none": result.toString()) +
          ".  Files replaced " + compactedFiles.toString() +
          " some of which may have been already removed", e);
      }
      // 4. Compute new store size
      this.storeSize = 0L;
      for (StoreFile hsf : this.storefiles) {
        StoreFile.Reader r = hsf.getReader();
        if (r == null) {
          LOG.warn("StoreFile " + hsf + " has a null Reader");
          continue;
        }
        this.storeSize += r.length();
      }
    } finally {
      this.lock.writeLock().unlock();
    }
    return result;
  }

  // ////////////////////////////////////////////////////////////////////////////
  // Accessors.
  // (This is the only section that is directly useful!)
  //////////////////////////////////////////////////////////////////////////////
  /**
   * @return the number of files in this store
   */
  public int getNumberOfstorefiles() {
    return this.storefiles.size();
  }

  /*
   * @param wantedVersions How many versions were asked for.
   * @return wantedVersions or this families' {@link HConstants#VERSIONS}.
   */
  int versionsToReturn(final int wantedVersions) {
    if (wantedVersions <= 0) {
      throw new IllegalArgumentException("Number of versions must be > 0");
    }
    // Make sure we do not return more than maximum versions for this store.
    int maxVersions = this.family.getMaxVersions();
    return wantedVersions > maxVersions ? maxVersions: wantedVersions;
  }

  static boolean isExpired(final KeyValue key, final long oldestTimestamp) {
    return key.getTimestamp() < oldestTimestamp;
  }

  /**
   * Find the key that matches <i>row</i> exactly, or the one that immediately
   * preceeds it. WARNING: Only use this method on a table where writes occur
   * with strictly increasing timestamps. This method assumes this pattern of
   * writes in order to make it reasonably performant.  Also our search is
   * dependent on the axiom that deletes are for cells that are in the container
   * that follows whether a memstore snapshot or a storefile, not for the
   * current container: i.e. we'll see deletes before we come across cells we
   * are to delete. Presumption is that the memstore#kvset is processed before
   * memstore#snapshot and so on.
   * @param kv First possible item on targeted row; i.e. empty columns, latest
   * timestamp and maximum type.
   * @return Found keyvalue or null if none found.
   * @throws IOException
   */
  KeyValue getRowKeyAtOrBefore(final KeyValue kv) throws IOException {
    GetClosestRowBeforeTracker state = new GetClosestRowBeforeTracker(
      this.comparator, kv, this.ttl, this.region.getRegionInfo().isMetaRegion());
    this.lock.readLock().lock();
    try {
      // First go to the memstore.  Pick up deletes and candidates.
      this.memstore.getRowKeyAtOrBefore(state);
      // Check if match, if we got a candidate on the asked for 'kv' row.
      // Process each store file. Run through from newest to oldest.
      for (StoreFile sf : Iterables.reverse(storefiles)) {
        // Update the candidate keys from the current map file
        rowAtOrBeforeFromStoreFile(sf, state);
      }
      return state.getCandidate();
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /*
   * Check an individual MapFile for the row at or before a given row.
   * @param f
   * @param state
   * @throws IOException
   */
  private void rowAtOrBeforeFromStoreFile(final StoreFile f,
                                          final GetClosestRowBeforeTracker state)
      throws IOException {
    StoreFile.Reader r = f.getReader();
    if (r == null) {
      LOG.warn("StoreFile " + f + " has a null Reader");
      return;
    }
    // TODO: Cache these keys rather than make each time?
    byte [] fk = r.getFirstKey();
    KeyValue firstKV = KeyValue.createKeyValueFromKey(fk, 0, fk.length);
    byte [] lk = r.getLastKey();
    KeyValue lastKV = KeyValue.createKeyValueFromKey(lk, 0, lk.length);
    KeyValue firstOnRow = state.getTargetKey();
    if (this.comparator.compareRows(lastKV, firstOnRow) < 0) {
      // If last key in file is not of the target table, no candidates in this
      // file.  Return.
      if (!state.isTargetTable(lastKV)) return;
      // If the row we're looking for is past the end of file, set search key to
      // last key. TODO: Cache last and first key rather than make each time.
      firstOnRow = new KeyValue(lastKV.getRow(), HConstants.LATEST_TIMESTAMP);
    }
    // Get a scanner that caches blocks and that uses pread.
    HFileScanner scanner = r.getScanner(true, true);
    // Seek scanner.  If can't seek it, return.
    if (!seekToScanner(scanner, firstOnRow, firstKV)) return;
    // If we found candidate on firstOnRow, just return. THIS WILL NEVER HAPPEN!
    // Unlikely that there'll be an instance of actual first row in table.
    if (walkForwardInSingleRow(scanner, firstOnRow, state)) return;
    // If here, need to start backing up.
    while (scanner.seekBefore(firstOnRow.getBuffer(), firstOnRow.getKeyOffset(),
       firstOnRow.getKeyLength())) {
      KeyValue kv = scanner.getKeyValue();
      if (!state.isTargetTable(kv)) break;
      if (!state.isBetterCandidate(kv)) break;
      // Make new first on row.
      firstOnRow = new KeyValue(kv.getRow(), HConstants.LATEST_TIMESTAMP);
      // Seek scanner.  If can't seek it, break.
      if (!seekToScanner(scanner, firstOnRow, firstKV)) break;
      // If we find something, break;
      if (walkForwardInSingleRow(scanner, firstOnRow, state)) break;
    }
  }

  /*
   * Seek the file scanner to firstOnRow or first entry in file.
   * @param scanner
   * @param firstOnRow
   * @param firstKV
   * @return True if we successfully seeked scanner.
   * @throws IOException
   */
  private boolean seekToScanner(final HFileScanner scanner,
                                final KeyValue firstOnRow,
                                final KeyValue firstKV)
      throws IOException {
    KeyValue kv = firstOnRow;
    // If firstOnRow < firstKV, set to firstKV
    if (this.comparator.compareRows(firstKV, firstOnRow) == 0) kv = firstKV;
    int result = scanner.seekTo(kv.getBuffer(), kv.getKeyOffset(),
      kv.getKeyLength());
    return result >= 0;
  }

  /*
   * When we come in here, we are probably at the kv just before we break into
   * the row that firstOnRow is on.  Usually need to increment one time to get
   * on to the row we are interested in.
   * @param scanner
   * @param firstOnRow
   * @param state
   * @return True we found a candidate.
   * @throws IOException
   */
  private boolean walkForwardInSingleRow(final HFileScanner scanner,
                                         final KeyValue firstOnRow,
                                         final GetClosestRowBeforeTracker state)
      throws IOException {
    boolean foundCandidate = false;
    do {
      KeyValue kv = scanner.getKeyValue();
      // If we are not in the row, skip.
      if (this.comparator.compareRows(kv, firstOnRow) < 0) continue;
      // Did we go beyond the target row? If so break.
      if (state.isTooFar(kv, firstOnRow)) break;
      if (state.isExpired(kv)) {
        continue;
      }
      // If we added something, this row is a contender. break.
      if (state.handle(kv)) {
        foundCandidate = true;
        break;
      }
    } while(scanner.next());
    return foundCandidate;
  }

  /**
   * Determines if HStore can be split
   * @param force Whether to force a split or not.
   * @return a StoreSize if store can be split, null otherwise.
   */
  StoreSize checkSplit(final boolean force) {
    this.lock.readLock().lock();
    try {
      // Iterate through all store files
      if (this.storefiles.isEmpty()) {
        return null;
      }
      if (!force && (storeSize < this.desiredMaxFileSize)) {
        return null;
      }

      if (this.region.getRegionInfo().isMetaRegion()) {
        if (force) {
          LOG.warn("Cannot split meta regions in HBase 0.20");
        }
        return null;
      }

      // Not splitable if we find a reference store file present in the store.
      boolean splitable = true;
      long maxSize = 0L;
      StoreFile largestSf = null;
      for (StoreFile sf : storefiles) {
        if (splitable) {
          splitable = !sf.isReference();
          if (!splitable) {
            // RETURN IN MIDDLE OF FUNCTION!!! If not splitable, just return.
            if (LOG.isDebugEnabled()) {
              LOG.debug(sf +  " is not splittable");
            }
            return null;
          }
        }
        StoreFile.Reader r = sf.getReader();
        if (r == null) {
          LOG.warn("Storefile " + sf + " Reader is null");
          continue;
        }
        long size = r.length();
        if (size > maxSize) {
          // This is the largest one so far
          maxSize = size;
          largestSf = sf;
        }
      }
      StoreFile.Reader r = largestSf.getReader();
      if (r == null) {
        LOG.warn("Storefile " + largestSf + " Reader is null");
        return null;
      }
      // Get first, last, and mid keys.  Midkey is the key that starts block
      // in middle of hfile.  Has column and timestamp.  Need to return just
      // the row we want to split on as midkey.
      byte [] midkey = r.midkey();
      if (midkey != null) {
        KeyValue mk = KeyValue.createKeyValueFromKey(midkey, 0, midkey.length);
        byte [] fk = r.getFirstKey();
        KeyValue firstKey = KeyValue.createKeyValueFromKey(fk, 0, fk.length);
        byte [] lk = r.getLastKey();
        KeyValue lastKey = KeyValue.createKeyValueFromKey(lk, 0, lk.length);
        // if the midkey is the same as the first and last keys, then we cannot
        // (ever) split this region.
        if (this.comparator.compareRows(mk, firstKey) == 0 &&
            this.comparator.compareRows(mk, lastKey) == 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("cannot split because midkey is the same as first or " +
              "last row");
          }
          return null;
        }
        return new StoreSize(maxSize, mk.getRow());
      }
    } catch(IOException e) {
      LOG.warn("Failed getting store size for " + this.storeNameStr, e);
    } finally {
      this.lock.readLock().unlock();
    }
    return null;
  }

  /** @return aggregate size of HStore */
  public long getSize() {
    return storeSize;
  }

  //////////////////////////////////////////////////////////////////////////////
  // File administration
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Return a scanner for both the memstore and the HStore files
   * @throws IOException
   */
  public KeyValueScanner getScanner(Scan scan,
      final NavigableSet<byte []> targetCols) throws IOException {
    lock.readLock().lock();
    try {
      return new StoreScanner(this, scan, targetCols);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    return this.storeNameStr;
  }

  /**
   * @return Count of store files
   */
  int getStorefilesCount() {
    return this.storefiles.size();
  }

  /**
   * @return The size of the store files, in bytes.
   */
  long getStorefilesSize() {
    long size = 0;
    for (StoreFile s: storefiles) {
      StoreFile.Reader r = s.getReader();
      if (r == null) {
        LOG.warn("StoreFile " + s + " has a null Reader");
        continue;
      }
      size += r.length();
    }
    return size;
  }

  /**
   * @return The size of the store file indexes, in bytes.
   */
  long getStorefilesIndexSize() {
    long size = 0;
    for (StoreFile s: storefiles) {
      StoreFile.Reader r = s.getReader();
      if (r == null) {
        LOG.warn("StoreFile " + s + " has a null Reader");
        continue;
      }
      size += r.indexSize();
    }
    return size;
  }

  /**
   * Datastructure that holds size and row to split a file around.
   * TODO: Take a KeyValue rather than row.
   */
  static class StoreSize {
    private final long size;
    private final byte [] row;

    StoreSize(long size, byte [] row) {
      this.size = size;
      this.row = row;
    }
    /* @return the size */
    long getSize() {
      return size;
    }

    byte [] getSplitRow() {
      return this.row;
    }
  }

  HRegion getHRegion() {
    return this.region;
  }

  HRegionInfo getHRegionInfo() {
    return this.region.regionInfo;
  }

  /**
   * Increments the value for the given row/family/qualifier.
   *
   * This function will always be seen as atomic by other readers
   * because it only puts a single KV to memstore. Thus no
   * read/write control necessary.
   *
   * @param row
   * @param f
   * @param qualifier
   * @param newValue the new value to set into memstore
   * @return memstore size delta
   * @throws IOException
   */
  public long updateColumnValue(byte [] row, byte [] f,
                                byte [] qualifier, long newValue)
      throws IOException {
    return updateColumnValue(row, f, qualifier, Bytes.toBytes(newValue));
  }

  /**
   * Increments the value for the given row/family/qualifier.
   *
   * This function will always be seen as atomic by other readers
   * because it only puts a single KV to memstore. Thus no
   * read/write control necessary.
   *
   * @param row
   * @param f
   * @param qualifier
   * @param newValue the new value to set into memstore
   * @return memstore size delta
   * @throws IOException
   */
  public long updateColumnValue(byte [] row, byte [] f,
                                byte [] qualifier, byte [] newValue)
      throws IOException {

    this.lock.readLock().lock();
    try {
      long now = EnvironmentEdgeManager.currentTimeMillis();

      return this.memstore.updateColumnValue(row,
          f,
          qualifier,
          newValue,
          now);

    } finally {
      this.lock.readLock().unlock();
    }
  }

  public StoreFlusher getStoreFlusher(long cacheFlushId) {
    return new StoreFlusherImpl(cacheFlushId);
  }

  private class StoreFlusherImpl implements StoreFlusher {

    private long cacheFlushId;
    private SortedSet<KeyValue> snapshot;
    private StoreFile storeFile;
    private TimeRangeTracker snapshotTimeRangeTracker;

    private StoreFlusherImpl(long cacheFlushId) {
      this.cacheFlushId = cacheFlushId;
    }

    @Override
    public void prepare() {
      memstore.snapshot();
      this.snapshot = memstore.getSnapshot();
      this.snapshotTimeRangeTracker = memstore.getSnapshotTimeRangeTracker();
    }

    @Override
    public void flushCache() throws IOException {
      storeFile = Store.this.flushCache(cacheFlushId, snapshot, snapshotTimeRangeTracker);
    }

    @Override
    public boolean commit() throws IOException {
      if (storeFile == null) {
        return false;
      }
      // Add new file to store files.  Clear snapshot too while we have
      // the Store write lock.
      return Store.this.updateStorefiles(storeFile, snapshot);
    }
  }

  /**
   * See if there's too much store files in this store
   * @return true if number of store files is greater than
   *  the number defined in compactionThreshold
   */
  public boolean hasTooManyStoreFiles() {
    return this.storefiles.size() > this.compactionThreshold;
  }

  public static final long FIXED_OVERHEAD = ClassSize.align(
      ClassSize.OBJECT + (14 * ClassSize.REFERENCE) +
      (4 * Bytes.SIZEOF_LONG) + (3 * Bytes.SIZEOF_INT) + (Bytes.SIZEOF_BOOLEAN * 2));

  public static final long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD +
      ClassSize.OBJECT + ClassSize.REENTRANT_LOCK +
      ClassSize.CONCURRENT_SKIPLISTMAP +
      ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + ClassSize.OBJECT);

  @Override
  public long heapSize() {
    return DEEP_OVERHEAD + this.memstore.heapSize();
  }
}
