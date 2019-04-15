/*
 * Copyright 2018 Longtail Ad Solutions (DBA JW Player)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jwplayer.southpaw.state;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.jwplayer.southpaw.metric.Metrics;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.FileHelper;
import com.jwplayer.southpaw.util.S3Helper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;


/**
 * Rocks DB implementation for the state
 */
public class RocksDBState extends BaseState {
    private static final Logger logger = Logger.getLogger(RocksDBState.class);
    /**
     * URI to backup RocksDB to
     */
    public static final String BACKUP_URI_CONFIG = "rocks.db.backup.uri";
    /**
     * If true rollback to previous rocksdb backup upon state restoration corruption
     */
    public static final String BACKUPS_AUTO_ROLLBACK_CONFIG = "rocks.db.backups.auto.rollback";
    /**
     * The # of backups to keep
     */
    public static final String BACKUPS_TO_KEEP_CONFIG = "rocks.db.backups.to.keep";
    /**
     * Amount of memory used by the compaction process
     */
    public static final String COMPACTION_READ_AHEAD_SIZE_CONFIG = "rocks.db.compaction.read.ahead.size";
    /**
     * Sets the max # of background compaction threads
     */
    public static final String MAX_BACKGROUND_COMPACTIONS = "rocks.db.max.background.compactions";
    /**
     * Sets the max # of background flush threads
     */
    public static final String MAX_BACKGROUND_FLUSHES = "rocks.db.max.background.flushes";
    /**
     * Sets the max # of threads used for subcompactions
     */
    public static final String MAX_SUBCOMPACTIONS = "rocks.db.max.subcompactions";
    /**
     * Sets the max # of write buffers
     */
    public static final String MAX_WRITE_BUFFER_NUMBER = "rocks.db.max.write.buffer.number";
    /**
     * Amount of memory used for memtables
     */
    public static final String MEMTABLE_SIZE = "rocks.db.memtable.size";
    /**
     * Used for # of threads for RocksDB parallelism
     */
    public static final String PARALLELISM_CONFIG = "rocks.db.parallelism";
    /**
     * How many puts are batched before automatically being flushed
     */
    public static final String PUT_BATCH_SIZE = "rocks.db.put.batch.size";
    /**
     * When a restore should be run. Expects a value of {@link RestoreMode}
     */
    public static final String RESTORE_MODE_CONFIG = "rocks.db.restore.mode";
    /**
     * URI for RocksDB
     */
    public static final String URI_CONFIG = "rocks.db.uri";

    public static class Iterator extends BaseState.Iterator {
        RocksIterator innerIter;

        public Iterator(RocksIterator iter) {
            this.innerIter = iter;
            iter.seekToFirst();
        }

        @Override
        public void close() {
            innerIter.close();
        }

        @Override
        public boolean hasNext() {
            return innerIter.isValid();
        }

        @Override
        public AbstractMap.SimpleEntry<byte[], byte[]> next() {
            AbstractMap.SimpleEntry<byte[], byte[]> retVal = new AbstractMap.SimpleEntry<>(innerIter.key(), innerIter.value());
            innerIter.next();
            return retVal;
        }

        @Override
        public void reset() {
            innerIter.seekToFirst();
        }
    }

    /**
     * Backup URI
     */
    protected URI backupURI;
    /**
     * Auto rollback to previous backups on restoration failure
     */
    protected boolean backupsAutoRollback;
    /**
     * The # of backups to keep
     */
    protected int backupsToKeep;
    /**
     * The size in bytes of the compaction read ahead
     */
    protected int compactionReadAheadSize;
    /**
     * RocksDB column family handles.
     */
    protected Map<ByteArray, ColumnFamilyHandle> cfHandles = new HashMap<>();
    /**
     * Configuration for this state
     */
    protected Map<String, Object> config;
    /**
     * Max # of background compactions
     */
    protected int maxBackgroundCompactions;
    /**
     * Max # of background flushes
     */
    protected int maxBackgroundFlushes;
    /**
     * Max # of subcompactions
     */
    protected int maxSubcompactions;
    /**
     * Max # of write buffer size
     */
    protected int maxWriteBufferNumber;
    /**
     * Size of the RocksDB memory
     */
    protected long memtableSize;
    /**
     * Simple metrics class for RocksDBState
     */
    protected final Metrics metrics = new Metrics();
    /**
     * Used for # of threads / parallelism for various Rocks DB config options
     */
    protected int parallelism;
    /**
     * How many puts are batched before automatically being flushed
     */
    protected int putBatchSize;
    /**
     * When restores should be performed
     */
    protected RestoreMode restoreMode;
    /**
     * RocksDB itself
     */
    protected RocksDB rocksDB;
    /**
     * Rocks DB options
     */
    protected Options rocksDBOptions;
    /**
     * Rocks DB Flush options
     */
    protected FlushOptions flushOptions;
    /**
     * Rocks DB Write options
     */
    protected WriteOptions writeOptions;
    /**
     * S3 helper class for backups in S3
     */
    protected S3Helper s3Helper;
    /**
     * SST File Manager for RocksDB
     */
    protected SstFileManager sstFileManager;
    /**
     * RocksDB stats object
     */
    protected Statistics statistics;
    /**
     * The URI to the DB.
     */
    protected URI uri;
    /**
     * We use this object to store data before batching together to write. Calling flush commits them to RocksDB.
     */
    protected Map<ByteArray, Map<ByteArray, byte[]>> dataBatches = new HashMap<>();

    public RocksDBState() {
        RocksDB.loadLibrary();
    }

    public RocksDBState(Map<String, Object> config) {
        super(config);
        RocksDB.loadLibrary();
    }

    @Override
    public void backup() {
        logger.info("Backing up RocksDB state");
        try {
            switch(backupURI.getScheme().toLowerCase()) {
                case FileHelper.SCHEME:
                    backup(backupURI.getPath());
                    break;
                case S3Helper.SCHEME:
                    String localBackupPath = getLocalBackupPath(uri);
                    backup(localBackupPath);
                    s3Helper.syncToS3(new URI(localBackupPath), backupURI);
                    break;
                default:
                    throw new RuntimeException("Unsupported schema: " + backupURI.getScheme());
            }
        } catch(InterruptedException | ExecutionException | URISyntaxException | RocksDBException ex) {
            throw new RuntimeException(ex);
        }
        logger.info("RocksDB state backup complete");
    }

    /**
     * Backups the DB to a local path.
     * @param backupPath - The local backup path
     */
    protected void backup(String backupPath) throws RocksDBException {
        File file = new File(backupPath);
        if(!file.exists()) file.mkdir();
        logger.info("Opening RocksDB backup engine");
        try (final BackupableDBOptions backupOptions = new BackupableDBOptions(backupPath)
                .setShareTableFiles(true)
                .setMaxBackgroundOperations(parallelism);
            final BackupEngine backupEngine = BackupEngine.open(Env.getDefault(), backupOptions)) {
            backupEngine.createNewBackup(rocksDB, true);
            backupEngine.purgeOldBackups(backupsToKeep);
        }
    }

    @Override
    public void close() {
        if(!isOpen()) {
            return;
        }
        super.close();

        for(Map.Entry<ByteArray, ColumnFamilyHandle> entry: cfHandles.entrySet()) {
            entry.getValue().close();
        }
        cfHandles.clear();
        dataBatches.clear();
        rocksDBOptions.close();
        flushOptions.close();
        writeOptions.close();
        rocksDB.close();

        rocksDBOptions = null;
        flushOptions = null;
        writeOptions = null;
        rocksDB = null;
    }

    @Override
    public void configure(Map<String, Object> config) {
        try {
            this.config = Preconditions.checkNotNull(config);
            this.backupURI = new URI(Preconditions.checkNotNull(config.get(BACKUP_URI_CONFIG).toString()));
            this.backupsAutoRollback = (boolean) config.getOrDefault(BACKUPS_AUTO_ROLLBACK_CONFIG, false);
            this.backupsToKeep = (int) Preconditions.checkNotNull(config.get(BACKUPS_TO_KEEP_CONFIG));
            this.compactionReadAheadSize = (int) Preconditions.checkNotNull(config.get(COMPACTION_READ_AHEAD_SIZE_CONFIG));
            this.maxBackgroundCompactions = (int) config.getOrDefault(MAX_BACKGROUND_COMPACTIONS, 1);
            this.maxBackgroundFlushes = (int) config.getOrDefault(MAX_BACKGROUND_FLUSHES, 1);
            this.maxSubcompactions = (int) config.getOrDefault(MAX_SUBCOMPACTIONS, 1);
            this.maxWriteBufferNumber = (int) config.getOrDefault(MAX_WRITE_BUFFER_NUMBER, 1);
            this.memtableSize = ((Number) Preconditions.checkNotNull(config.get(MEMTABLE_SIZE))).longValue();
            this.parallelism = (int) config.getOrDefault(PARALLELISM_CONFIG, 1);
            this.putBatchSize = (int) Preconditions.checkNotNull(config.get(PUT_BATCH_SIZE));
            this.restoreMode = RestoreMode.parse(config.getOrDefault(RESTORE_MODE_CONFIG, RestoreMode.NEVER.getValue()).toString());
            this.uri = new URI(Preconditions.checkNotNull(config.get(URI_CONFIG).toString()));

            if(backupURI.getScheme().toLowerCase().equals(S3Helper.SCHEME)) {
                s3Helper = new S3Helper(config);
            }
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void open() {
        if(isOpen()){
            throw new RuntimeException("RocksDB is already open!");
        }
        try {
            // Create the backing DB
            sstFileManager = new SstFileManager(Env.getDefault());
            statistics = new Statistics();
            DBOptions dbOptions = new DBOptions()
                    .setCreateIfMissing(false) // Explicitly set to false here so we can potentially restore if it doesn't exist
                    .setCreateMissingColumnFamilies(true)
                    .setIncreaseParallelism(parallelism)
                    .setMaxBackgroundCompactions(maxBackgroundCompactions)
                    .setMaxBackgroundFlushes(maxBackgroundFlushes)
                    .setWalSizeLimitMB(0L)
                    .setMaxTotalWalSize(0L)
                    .setStatistics(statistics)
                    .setSstFileManager(sstFileManager);
            dbOptions.setMaxSubcompactions(maxSubcompactions);
            ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                    .setCompactionStyle(CompactionStyle.LEVEL)
                    .setNumLevels(4)
                    .setMaxWriteBufferNumber(maxWriteBufferNumber)
                    .setTargetFileSizeMultiplier(2);
            rocksDBOptions = new Options(dbOptions, cfOptions)
                    .setCreateIfMissing(false) // Explicitly set to false here so we can potentially restore if it doesn't exist
                    .setCreateMissingColumnFamilies(true)
                    .optimizeLevelStyleCompaction(memtableSize)

                    .setCompactionStyle(CompactionStyle.LEVEL)
                    .setNumLevels(4)
                    .setIncreaseParallelism(parallelism)
                    .setCompactionReadaheadSize(compactionReadAheadSize)
                    .setMaxBackgroundCompactions(maxBackgroundCompactions)
                    .setMaxBackgroundFlushes(maxBackgroundFlushes)
                    .setMaxWriteBufferNumber(maxWriteBufferNumber)
                    .setWalSizeLimitMB(0L)
                    .setWalTtlSeconds(0L)
                    .setTargetFileSizeMultiplier(2)
                    .setSstFileManager(sstFileManager)
                    .setStatistics(statistics);
            rocksDBOptions.setMaxSubcompactions(maxSubcompactions);

            flushOptions = new FlushOptions();
            flushOptions.setWaitForFlush(true);

            writeOptions = new WriteOptions();
            writeOptions.setDisableWAL(false);

            if (restoreMode == RestoreMode.ALWAYS){
                this.restore();
            }

            List<byte[]> families = RocksDB.listColumnFamilies(rocksDBOptions, uri.getPath());
            List<ColumnFamilyHandle> handles = new ArrayList<>(families.size() + 1);
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>(families.size() + 1);
            for (byte[] family : families) {
                descriptors.add(new ColumnFamilyDescriptor(family, cfOptions));
            }
            if (descriptors.size() == 0) {
                descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
            }

            try {
                rocksDB = RocksDB.open(dbOptions, uri.getPath(), descriptors, handles);
            } catch(RocksDBException ex) {
                if(!ex.getMessage().contains("does not exist (create_if_missing is false)")) {
                    // Received an unexpected exception
                    throw ex;
                }

                //DB must not exist yet.
                if (restoreMode == RestoreMode.WHEN_NEEDED) {
                    this.restore();

                    families = RocksDB.listColumnFamilies(rocksDBOptions, uri.getPath());
                    handles = new ArrayList<>(families.size() + 1);
                    descriptors = new ArrayList<>(families.size() + 1);
                    for (byte[] family : families) {
                        descriptors.add(new ColumnFamilyDescriptor(family, cfOptions));
                    }
                    if (descriptors.size() == 0) {
                        descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
                    }
                }

                dbOptions.setCreateIfMissing(true); // Auto create db if no db exists at this point
                rocksDB = RocksDB.open(dbOptions, uri.getPath(), descriptors, handles);
            }

            for (int i = 0; i < families.size(); i++) {
                if (i > 0) {
                    ByteArray byteArray = new ByteArray(families.get(i));
                    dataBatches.put(byteArray, new HashMap<>());
                }
            }
            for(ColumnFamilyHandle handle: handles) {
                cfHandles.put(new ByteArray(handle.getName()), handle);
            }
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
        super.open();
    }

    @Override
    public void createKeySpace(String keySpace) {
        ByteArray handleName = new ByteArray(keySpace);
        if(!cfHandles.containsKey(handleName)) {
            createKeySpace(handleName);
        }
    }

    protected void createKeySpace(ByteArray handleName) {
        ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(handleName.getBytes());
        ColumnFamilyHandle cfHandle;
        try {
            cfHandle = this.rocksDB.createColumnFamily(cfDescriptor);
        } catch (RocksDBException ex) {
            throw new RuntimeException(ex);
        }
        cfHandles.put(handleName, cfHandle);
        dataBatches.put(handleName, new HashMap<>());
    }

    @Override
    public void delete() throws RuntimeException{
        logger.info("Deleting RocksDB state");
        if(isOpen()) {
            throw new RuntimeException("RocksDB is currently open. Must call close() first.");
        }

        if (uri != null && new File(uri).exists()){
            try (final Options options = new Options();){
                RocksDB.destroyDB(uri.getPath(), options);
            } catch(RocksDBException ex) {
                throw new RuntimeException(ex);
            }
        }
        metrics.statesDeleted.mark();
        logger.info("RocksDB state has been deleted");
    }

    @Override
    public void delete(String keySpace, byte[] key) {
        ByteArray handleName = new ByteArray(keySpace);
        Preconditions.checkNotNull(cfHandles.get(handleName));
        try {
            dataBatches.get(handleName).remove(new ByteArray(key));
            rocksDB.delete(cfHandles.get(handleName), writeOptions, key);
        } catch(RocksDBException ex) {
            logger.error("Problem deleting RocksDB record, keySpace: " + keySpace + ", key: " + Hex.encodeHexString(key));
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void deleteBackups() {
        logger.info("Deleting RocksDB state backups");
        try {
            File file;
            switch(backupURI.getScheme().toLowerCase()) {
                case FileHelper.SCHEME:
                    file = new File(backupURI);
                    FileUtils.deleteDirectory(file);
                    break;
                case S3Helper.SCHEME:
                    String localBackupPath = getLocalBackupPath(uri);
                    file = new File(localBackupPath);
                    FileUtils.deleteDirectory(file);
                    s3Helper.deleteKeys(backupURI);
                    break;
                default:
                    throw new RuntimeException("Unsupported schema: " + backupURI.getScheme());
            }
        } catch(IOException | InterruptedException | ExecutionException ex) {
            throw new RuntimeException(ex);
        }
        metrics.backupsDeleted.mark();
        logger.info("RocksDB state backups have been deleted");
    }

    @Override
    public void flush() {
        try {
            for(Map.Entry<ByteArray, Map<ByteArray, byte[]>> entry: dataBatches.entrySet()) {
                putBatch(entry.getKey());
            }
            rocksDB.flush(flushOptions);
        } catch(RocksDBException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void flush(String keySpace) {
        Preconditions.checkNotNull(keySpace);
        ByteArray byteArray = new ByteArray(keySpace);
        try {
            putBatch(byteArray);
            rocksDB.flush(flushOptions, cfHandles.get(byteArray));
        } catch(RocksDBException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public byte[] get(String keySpace, byte[] key) {
        ByteArray handleName = new ByteArray(keySpace);
        Preconditions.checkNotNull(cfHandles.get(handleName));
        try {
            byte[] retVal = dataBatches.get(new ByteArray(keySpace)).get(new ByteArray(key));
            if(retVal == null) {
                retVal = rocksDB.get(cfHandles.get(handleName), key);
            }
            return retVal;
        } catch(RocksDBException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Gets a local backup path using the DB URI
     * @param uri - The (local) location to the DB
     * @return A path to store local backups in
     */
    protected String getLocalBackupPath(URI uri) {
        Preconditions.checkNotNull(uri);

        if(uri.getPath().endsWith("/")) {
            return uri.getPath() + "backups";
        } else {
            return uri.getPath() + "/" + "backups";
        }
    }

    @Override
    public Iterator iterate(String keySpace) {
        // NOTE: This only iterates RocksDB, not the appropriate data batch. Call flush first if
        // you need to iterate everything.
        ByteArray handleName = new ByteArray(keySpace);
        Preconditions.checkNotNull(cfHandles.get(handleName));
        return new Iterator(rocksDB.newIterator(cfHandles.get(handleName)));
    }

    @Override
    public void put(String keySpace, byte[] key, byte[] value) {
        Preconditions.checkNotNull(key);
        ByteArray byteArray = new ByteArray(keySpace);
        Map<ByteArray, byte[]> dataBatch = Preconditions.checkNotNull(dataBatches.get(byteArray));
        dataBatch.put(new ByteArray(key), value);
        if(dataBatch.size() >= putBatchSize) {
            putBatch(byteArray);
        }
    }

    protected void putBatch(ByteArray keySpace) {
        Map<ByteArray, byte[]> dataBatch = dataBatches.get(keySpace);
        try (WriteBatch writeBatch = new WriteBatch()){
            for(Map.Entry<ByteArray, byte[]> batchEntry: dataBatch.entrySet()) {
                writeBatch.put(
                        cfHandles.get(keySpace),
                        batchEntry.getKey().getBytes(),
                        batchEntry.getValue()
                );
            }
            rocksDB.write(writeOptions, writeBatch);
            dataBatch.clear();
        } catch(RocksDBException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void restore() {
        logger.info("Restoring RocksDB state from backups");
        try(Timer.Context context = metrics.backupsRestored.time()) {
            try {
                switch (backupURI.getScheme().toLowerCase()) {
                    case FileHelper.SCHEME:
                        restore(uri, backupURI.getPath());
                        break;
                    case S3Helper.SCHEME:
                        String localBackupPath = getLocalBackupPath(uri);
                        File file = new File(localBackupPath);
                        if (!file.exists()) file.mkdir();
                        s3Helper.syncFromS3(new URI(localBackupPath), backupURI);
                        restore(uri, localBackupPath);
                        break;
                    default:
                        throw new RuntimeException("Unsupported schema: " + backupURI.getScheme());
                }
            } catch (InterruptedException | ExecutionException | RocksDBException | URISyntaxException ex) {
                throw new RuntimeException(ex);
            }
        }
        logger.info("RocksDB state restoration complete");
    }

    /**
     * Restore from a local path
     * @param dbUri - URI to where the DB is located
     * @param backupPath - Local path to the DB backups
     * @throws RocksDBException -
     */
    protected void restore(URI dbUri, String backupPath) throws RocksDBException {
        File path = new File(backupPath);
        if(path.exists()) {
            try (final BackupableDBOptions backupOptions = new BackupableDBOptions(backupPath)
                    .setShareTableFiles(true)
                    .setMaxBackgroundOperations(parallelism);
                 final RestoreOptions restoreOptions = new RestoreOptions(false);
                 final BackupEngine backupEngine = BackupEngine.open(Env.getDefault(), backupOptions)) {

                final List<BackupInfo> backupInfo = backupEngine.getBackupInfo();

                final int backups = backupInfo.size();
                if(backups > 0) {
                    delete();
                    if(backupsAutoRollback) {
                        boolean restored = false;
                        int backupIndex = backups - 1;
                        while (!restored){
                            logger.info("Attempting to restore backup " + (backups - backupIndex) + " of " + backups);
                            try {
                                backupEngine.restoreDbFromBackup(backupInfo.get(backupIndex).backupId(),
                                        dbUri.getPath(),
                                        dbUri.getPath(),
                                        restoreOptions);
                            } catch (RocksDBException ex) {
                                logger.warn("Failed to restore backup " + (backups - backupIndex) + " of "
                                        + backups + " with exception: " + ex.getMessage());
                                // Delete the corrupted backup
                                backupEngine.deleteBackup(backupInfo.get(backupIndex).backupId());
                                backupIndex -= 1;
                                if (backupIndex < 0) {
                                    logger.error("Failed to restore all backups");
                                    throw ex;
                                }
                                continue;
                            }
                            restored = true;
                        }
                    } else {
                        logger.info("Attempting to restore latest backup");
                        backupEngine.restoreDbFromLatestBackup(dbUri.getPath(), dbUri.getPath(), restoreOptions);
                    }
                    backupEngine.purgeOldBackups(backupsToKeep);
                    logger.info("Backup restored");
                } else {
                    logger.warn("Skipping state restore, no backups found in backup URI");
                }
            }
        } else {
            logger.warn("Skipping state restore, backup URI is empty");
        }
    }
}
