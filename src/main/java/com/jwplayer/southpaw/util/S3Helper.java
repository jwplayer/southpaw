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
package com.jwplayer.southpaw.util;

import com.amazonaws.auth.*;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * Helper class for accessing and using S3
 */
public class S3Helper {
    protected class Metrics {
        public static final String S3_DOWNLOADS = "s3.downloads";
        public static final String S3_FILES_DELETED = "s3.files.deleted";
        public static final String S3_FILES_DOWNLOADED = "s3.files.downloaded";
        public static final String S3_FILES_UPLOADED = "s3.files.uploaded";
        public static final String S3_UPLOAD_FAILURES = "s3.upload.failures";
        public static final String S3_UPLOADS = "s3.uploads";

        /**
         * Registry where metrics are registered
         */
        protected final MetricRegistry registry = new MetricRegistry();
        /**
         * Send the metrics to JMX.
         */
        protected final JmxReporter reporter = JmxReporter.forRegistry(registry).inDomain(com.jwplayer.southpaw.metric.Metrics.PREFIX).build();
        public final Timer s3Downloads = registry.timer(S3_DOWNLOADS);
        public final Meter s3FilesDeleted = registry.meter(S3_FILES_DELETED);
        public final Meter s3FilesDownloaded = registry.meter(S3_FILES_DOWNLOADED);
        public final Meter s3FilesUploaded = registry.meter(S3_FILES_UPLOADED);
        public final Meter s3UploadFailures = registry.meter(S3_UPLOAD_FAILURES);
        public final Timer s3Uploads = registry.timer(S3_UPLOADS);

        public Metrics() {
            reporter.start();
        }
    }

    /**
     * AWS access key config
     */
    public static final String ACCESS_KEY_ID_CONFIG = "aws.s3.access.key.id";
    /**
     * Determines if errors in syncs to S3 cause an exception or return with the sync unfinished. 'Ignored' errors
     * will be logged.
     */
    public static final String EXCEPTION_ON_ERROR_CONFIG = "aws.s3.exception.on.error";
    /**
     * By default, errors while syncing to S3 will result in an exception being thrown.
     */
    public static final boolean EXCEPTION_ON_ERROR_DEFAULT = true;
    /**
     * Maximum number of keys to include in a single AWS operation
     */
    public static final int MAX_KEYS_PER_S3_OP = 100;
    /**
     * AWS region config
     */
    public static final String REGION_CONFIG = "aws.s3.region";
    /**
     * The URI scheme for S3 URIs used by this class
     */
    public static final String SCHEME = "s3";
    /**
     * AWS secret key config
     */
    public static final String SECRET_KEY_CONFIG = "aws.s3.secret.key";
    private static final Logger logger = Logger.getLogger(S3Helper.class);

    private ExecutorService executor = Executors.newSingleThreadExecutor();
    protected boolean exceptionOnError;
    protected Metrics metrics = new Metrics();
    protected AmazonS3 s3;
    protected Future<?> syncToS3Future = null;

    /**
     * Constructor
     * @param config - Configuration map
     */
    public S3Helper(Map<String, Object> config) {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        if(config.containsKey(ACCESS_KEY_ID_CONFIG) && config.containsKey(SECRET_KEY_CONFIG)) {
            builder.withCredentials(
                    new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(
                                    config.get(ACCESS_KEY_ID_CONFIG).toString(),
                                    config.get(SECRET_KEY_CONFIG).toString())));
        } else {
            builder.withCredentials(DefaultAWSCredentialsProviderChain.getInstance());
        }
        if(config.containsKey(REGION_CONFIG)) {
            builder.withRegion(config.get(REGION_CONFIG).toString());
        } else {
            AwsRegionProvider regionProvider = new DefaultAwsRegionProviderChain();
            builder.withRegion(regionProvider.getRegion());
        }
        s3 = builder.build();
        exceptionOnError = (boolean) config.getOrDefault(EXCEPTION_ON_ERROR_CONFIG, EXCEPTION_ON_ERROR_DEFAULT);
    }

    /**
     * Constructor, useful for testing.
     * @param s3 - Prebuilt S3 client
     */
    public S3Helper(AmazonS3 s3) {
        this.s3 = s3;
    }

    /**
     * Deletes the keys for the given S3 URI. The URI can represent a prefix or a single key. This method will block
     * on an existing async sync to S3 before running.
     * @param s3Uri - The S3 URI (key or prefix) to delete
     */
    public void deleteKeys(URI s3Uri) throws InterruptedException, ExecutionException {
        waitForSyncToS3();
        Preconditions.checkNotNull(s3Uri);
        Preconditions.checkArgument(SCHEME.equalsIgnoreCase(s3Uri.getScheme()));
        List<S3ObjectSummary> summaries = listKeys(s3Uri);
        if(summaries.size() > 0) {
            String bucket = s3Uri.getHost();
            List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
            for (S3ObjectSummary summary : summaries) {
                keys.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
                if(keys.size() >= MAX_KEYS_PER_S3_OP) {
                    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket).withKeys(keys);
                    s3.deleteObjects(deleteObjectsRequest);
                    keys = new ArrayList<>();
                }
            }
            if(keys.size() > 0) {
                DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket).withKeys(keys);
                s3.deleteObjects(deleteObjectsRequest);
            }
        }
    }

    /**
     * Simple function for handling URI paths starting or ending with a /
     * @param uri - URI to get the path from
     * @return The path from the URI without a preceding or following /
     */
    protected static String getPath(URI uri) {
        String path = uri.getPath();
        path = path.startsWith("/") ? path.substring(1, path.length()) : path;
        path = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        return path;
    }

    protected static String getPath(File file) {
        String path = file.getPath();
        path = StringUtils.replaceChars(path, "\\", "/");
        path = path.startsWith("/") ? path.substring(1, path.length()) : path;
        path = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        return path;
    }

    /**
     * List all keys in S3 for the given URI as a prefix
     * @param s3Uri - The URI prefix
     * @return Returns a list of S3 object summaries
     */
    protected List<S3ObjectSummary> listKeys(URI s3Uri) {
        Preconditions.checkNotNull(s3Uri);
        Preconditions.checkArgument(SCHEME.equalsIgnoreCase(s3Uri.getScheme()));
        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(s3Uri.getHost())
                .withPrefix(getPath(s3Uri));
        ListObjectsV2Result result;
        List<S3ObjectSummary> retVal = new ArrayList<>();
        do {
            result = s3.listObjectsV2(request);
            retVal.addAll(result.getObjectSummaries());
            request.setContinuationToken(result.getNextContinuationToken());
        } while(result.isTruncated());
        return retVal;
    }

    /**
     * Syncs (copies) all files in S3 using the given S3 URI to the a local directory. This method will block
     * on an existing async sync to S3 before running.
     * @param localUri - The local directory to copy files to
     * @param s3Uri - The remote location of the files to copy
     * @throws InterruptedException -
     */
    public void syncFromS3(URI localUri, URI s3Uri) throws InterruptedException, ExecutionException {
        waitForSyncToS3();
        TransferManager tx = null;
        logger.info("Initiating sync from S3");
        try(Timer.Context context = metrics.s3Downloads.time()) {
            Preconditions.checkNotNull(localUri);
            Preconditions.checkNotNull(s3Uri);
            Preconditions.checkArgument(localUri.getScheme() == null || FileHelper.SCHEME.equalsIgnoreCase(localUri.getScheme()));
            Preconditions.checkArgument(SCHEME.equalsIgnoreCase(s3Uri.getScheme()));
            List<S3ObjectSummary> summaries = listKeys(s3Uri);
            String bucket = s3Uri.getHost();
            tx = TransferManagerBuilder.standard().withS3Client(s3).build();
            Map<File, Download> downloads = new HashMap<>();
            logger.info(String.format("Downloading files from %s to %s", s3Uri.toString(), localUri.toString()));
            int downloadCount = 0;

            // Download the files from S3
            for (S3ObjectSummary summary : summaries) {
                String suffix = StringUtils.substringAfter(summary.getKey(), getPath(s3Uri));
                File file = new File("/" + getPath(localUri) + suffix);

                file.getParentFile().mkdirs();
                downloads.put(file, tx.download(bucket, summary.getKey(), file));
                downloadCount++;
                metrics.s3FilesDownloaded.mark(1);
                if (downloadCount % MAX_KEYS_PER_S3_OP == 0) {
                    for (Map.Entry<File, Download> download : downloads.entrySet()) {
                        download.getValue().waitForCompletion();
                        download.getKey().setLastModified(
                                download.getValue().getObjectMetadata().getLastModified().getTime()
                        );
                    }
                    logger.info(String.format("Downloaded %s files", downloadCount));
                    downloads.clear();
                }
                file.setLastModified(summary.getLastModified().getTime());
            }
            if (downloads.size() >= 0) {
                for (Map.Entry<File, Download> download : downloads.entrySet()) {
                    download.getValue().waitForCompletion();
                    download.getKey().setLastModified(
                            download.getValue().getObjectMetadata().getLastModified().getTime()
                    );
                }
                logger.info(String.format("Downloaded %s files", downloadCount));
                downloads.clear();
            }
        } finally {
            if (tx != null) {
                tx.shutdownNow(false);
            }
        }
        logger.info("Sync from S3 complete");
    }

    /**
     * Syncs local files to S3 and cleans up the S3 path of files that do not exist locally. The sync itself is
     * asynchronous, but is tracked within this method. Subsequent calls will block until the previous sync is
     * finished.
     * @param localUri - The local directory to copy all files to S3
     * @param s3Uri - The remote S3 prefix to copy files to
     */
    public void syncToS3(URI localUri, URI s3Uri) throws InterruptedException, ExecutionException {
        waitForSyncToS3();
        logger.info("Initiating background sync to S3");
        syncToS3Future = executor.submit(() -> {
            try {
                try(Timer.Context context = metrics.s3Uploads.time()) {
                    Preconditions.checkNotNull(localUri);
                    Preconditions.checkArgument(localUri.getScheme() == null || FileHelper.SCHEME.equalsIgnoreCase(localUri.getScheme()));
                    Preconditions.checkArgument(SCHEME.equalsIgnoreCase(s3Uri.getScheme()));
                    Set<File> localFiles = FileHelper.listFiles(localUri);
                    List<S3ObjectSummary> summaries = listKeys(s3Uri);
                    String bucket = s3Uri.getHost();
                    logger.info(String.format("Uploading files from %s to %s", localUri.toString(), s3Uri.toString()));
                    int deleteCount = 0;
                    int uploadCount = 0;

                    // Get the files in S3 that are not local
                    List<S3ObjectSummary> s3ObjectsToDelete = new ArrayList<>();
                    for (S3ObjectSummary summary : summaries) {
                        String suffix = StringUtils.substringAfter(summary.getKey(), getPath(s3Uri));
                        boolean deleteMe = true;
                        for (File localFile : localFiles) {
                            String path = getPath(localFile);
                            if (path.endsWith(suffix)) {
                                deleteMe = false;
                                break;
                            }
                        }
                        if (deleteMe) s3ObjectsToDelete.add(summary);
                    }

                    // Get the files held locally, but are not in S3
                    List<File> localFilesToCopy = new ArrayList<>();
                    for (File localFile : localFiles) {
                        String suffix = StringUtils.substringAfter(getPath(localFile), getPath(localUri));
                        boolean copyMe = true;
                        for (S3ObjectSummary summary : summaries) {
                            if (summary.getKey().endsWith(suffix) && localFile.lastModified() <= summary.getLastModified().getTime()) {
                                copyMe = false;
                                break;
                            }
                        }
                        if (copyMe) localFilesToCopy.add(localFile);
                    }

                    // Copy the new/updated files to S3
                    if (localFilesToCopy.size() > 0) {
                        TransferManager tx = TransferManagerBuilder.standard().withS3Client(s3).build();
                        try{
                            List<Upload> uploads = new ArrayList<>();
                            for (File fileToCopy : localFilesToCopy) {
                                String fullPath = getPath(fileToCopy);
                                String suffix = StringUtils.substringAfter(fullPath, getPath(localUri));
                                String key = getPath(s3Uri) + suffix;
                                ObjectMetadata metadata = new ObjectMetadata();
                                metadata.setLastModified(new Date(fileToCopy.lastModified()));
                                PutObjectRequest request = new PutObjectRequest(bucket, key, fileToCopy).withMetadata(metadata);
                                uploads.add(tx.upload(request));
                                uploadCount++;
                                metrics.s3FilesUploaded.mark(1);
                                if (uploadCount % MAX_KEYS_PER_S3_OP == 0) {
                                    for (Upload upload : uploads) {
                                        upload.waitForUploadResult();
                                    }
                                    logger.info(String.format("Uploaded %s files", uploadCount));
                                    uploads.clear();
                                }
                            }
                            if (uploads.size() >= 0) {
                                for (Upload upload : uploads) {
                                    upload.waitForUploadResult();
                                }
                                logger.info(String.format("Uploaded %s files", uploadCount));
                                uploads.clear();
                            }
                        } finally {
                            tx.shutdownNow(false);
                        }
                    }

                    // Delete the extra S3 objects
                    if (s3ObjectsToDelete.size() > 0) {
                        List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
                        for (S3ObjectSummary summary : s3ObjectsToDelete) {
                            keys.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
                            deleteCount++;
                            metrics.s3FilesDeleted.mark(1);
                            if (deleteCount % MAX_KEYS_PER_S3_OP == 0) {
                                DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket).withKeys(keys);
                                s3.deleteObjects(deleteObjectsRequest);
                                logger.info(String.format("Deleted %s files", deleteCount));
                                keys.clear();
                            }
                        }
                        if (keys.size() > 0) {
                            DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket).withKeys(keys);
                            s3.deleteObjects(deleteObjectsRequest);
                            logger.info(String.format("Deleted %s files", deleteCount));
                        }
                    }
                }
            } catch(InterruptedException | URISyntaxException ex) {
                metrics.s3UploadFailures.mark(1);
                if(exceptionOnError) {
                    throw new RuntimeException(ex);
                } else {
                    logger.warn("Unhandled exception during sync to S3", ex);
                }
            }
            logger.info("Background sync to S3 complete");
        });
    }

    protected void waitForSyncToS3() throws InterruptedException, ExecutionException {
        if(syncToS3Future != null) {
            logger.info("Blocking for existing sync to S3 to complete");
            syncToS3Future.get();
            syncToS3Future = null;
            logger.info("Existing sync to S3 complete");
        }
    }
}
