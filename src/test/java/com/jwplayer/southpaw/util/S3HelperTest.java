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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.*;

import static org.junit.Assert.*;


public class S3HelperTest {
    protected String bucket = "bucket.com";
    protected URI s3Uri;
    protected S3Mock s3Mock;
    protected S3Helper s3;

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        s3Uri = new URI("s3://" + bucket + "/some/path");
        s3Mock = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
        s3Mock.start();
        AwsClientBuilder.EndpointConfiguration endpoint =
                new AwsClientBuilder.EndpointConfiguration("http://localhost:8001", "us-east-1");
        AmazonS3 client = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();
        client.createBucket(s3Uri.getHost());
        String prefix = S3Helper.getPath(s3Uri);
        client.putObject(s3Uri.getHost(), prefix + "/fileA.txt", "ABCD");
        client.putObject(s3Uri.getHost(), prefix + "/fileB.txt", "1234");
        client.putObject(s3Uri.getHost(), prefix + "/fileC.txt", "AB34");
        s3 = new S3Helper(client);
    }

    @After
    public void tearDown() {
        s3Mock.shutdown();
    }

    @Test
    public void deleteKeys() throws Exception {
        s3.deleteKeys(s3Uri);
        List<S3ObjectSummary> summaries = s3.listKeys(s3Uri);

        assertEquals(0, summaries.size());
    }

    @Test
    public void getPathWithFile() {
        File file = new File("/this/is/a/path/");
        String path = S3Helper.getPath(file);
        assertEquals("this/is/a/path", path);
    }

    @Test
    public void getPathWithURI() throws Exception {
        URI uri = new URI("file:///this/is/a/path/");
        String path = S3Helper.getPath(uri);
        assertEquals("this/is/a/path", path);
    }

    @Test
    public void listKeys() {
        List<S3ObjectSummary> summaries = s3.listKeys(s3Uri);
        List<String> keys = new ArrayList<>(summaries.size());
        for(S3ObjectSummary summary: summaries) keys.add(summary.getKey());
        keys.sort(Comparator.naturalOrder());

        assertEquals(3, keys.size());
        assertEquals("some/path/fileA.txt", keys.get(0));
        assertEquals("some/path/fileB.txt", keys.get(1));
        assertEquals("some/path/fileC.txt", keys.get(2));
    }

    @Test
    public void syncFromS3() throws Exception {
        s3.syncFromS3(tmpDir.getRoot().toURI(), s3Uri);
        Set<File> localFiles = FileHelper.listFiles(tmpDir.getRoot().toURI());
        List<String> fileNames = new ArrayList<>(localFiles.size());
        for(File localFile: localFiles) fileNames.add(localFile.getName());
        fileNames.sort(Comparator.naturalOrder());

        assertEquals(3, fileNames.size());
        assertTrue(fileNames.get(0).endsWith("fileA.txt"));
        assertTrue(fileNames.get(1).endsWith("fileB.txt"));
        assertTrue(fileNames.get(2).endsWith("fileC.txt"));
    }

    @Test
    public void syncToS3() throws Exception {
        File accountFile = tmpDir.newFile("account.txt");
        Files.write(accountFile.toPath(), "account".getBytes());

        File feedFile = tmpDir.newFile("feed.txt");
        Files.write(feedFile.toPath(), "feed".getBytes());

        File mediaFile = tmpDir.newFile("media.txt");
        Files.write(mediaFile.toPath(), "media".getBytes());

        URI backupUri = new URI("s3://" + bucket + "/backups");
        s3.syncToS3(tmpDir.getRoot().toURI(), backupUri);
        s3.waitForSyncToS3();

        feedFile.delete();

        File playerFile = tmpDir.newFile("player.txt");
        Files.write(playerFile.toPath(), "player".getBytes());

        s3.syncToS3(tmpDir.getRoot().toURI(), backupUri);
        s3.waitForSyncToS3();

        List<S3ObjectSummary> summaries = s3.listKeys(backupUri);
        List<String> keys = new ArrayList<>(summaries.size());
        for(S3ObjectSummary summary: summaries) keys.add(summary.getKey());
        keys.sort(Comparator.naturalOrder());

        assertEquals(3, keys.size());
        assertEquals("backups/" + accountFile.getName(), keys.get(0));
        assertEquals("backups/" + mediaFile.getName(), keys.get(1));
        assertEquals("backups/" + playerFile.getName(), keys.get(2));
    }
}
