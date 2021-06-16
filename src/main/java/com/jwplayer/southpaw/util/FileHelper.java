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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.IOUtils;


public class FileHelper {
  public static final String SCHEME = "file";

  public static InputStream getInputStream(URI uri) throws IOException, URISyntaxException {
    FileSystem fs = getFileSystem(uri);
    Path path = getPath(uri);
    return fs.provider().newInputStream(path);
  }

  /**
   * Gets the local/default file system from an absolute or relative URI
   *
   * @param uri - The URI to get the file system for
   * @return A file system for the URI
   * @throws URISyntaxException -
   */
  public static FileSystem getFileSystem(URI uri) throws URISyntaxException {
    URI fsURI;
    // This is a bit hacky because of weird behavior getting the file system.
    // We also want relative URIs to resolve to the default/local file system.
    if (uri.isAbsolute()) {
      Preconditions.checkArgument(uri.getScheme().equalsIgnoreCase("file"));
      fsURI = new URI(uri.getScheme(), uri.getAuthority() == null ? "" : uri.getAuthority(), "/", null, null);
    } else {
      fsURI = new URI("file", uri.getAuthority() == null ? "" : uri.getAuthority(), "/", null, null);
    }
    return FileSystems.getFileSystem(fsURI);
  }

  /**
   * Gets the path for a given URI, while Windows edge cases
   *
   * @param uri - URI to get the path for
   * @return A path for the URI
   * @throws URISyntaxException -
   */
  public static Path getPath(URI uri) throws URISyntaxException {
    FileSystem fs = getFileSystem(uri);
    Path path;

    // Windows paths can have a preceding slash that breaks this, even if Java gave it to us! :D
    if (
        System.getProperty("os.name").contains("indow")
            && uri.getPath().substring(0, 1).equals("/")
            && fs.provider().getScheme().equals("file")
            && uri.isAbsolute()
    ) {
      path = fs.getPath(uri.getPath().substring(1));
    } else {
      path = fs.getPath(uri.getPath());
    }

    return path;
  }

  /**
   * Gets all files from a given (local) URI, recursively.
   *
   * @param uri - A local URI (relative or absolute)
   * @return A list of files in/under the given
   */
  public static Set<File> listFiles(URI uri) throws URISyntaxException {
    Set<File> files = new HashSet<>();
    File file = getPath(Preconditions.checkNotNull(uri)).toFile();
    if (file.isDirectory()) {
      File[] filesArray = file.listFiles();
      if (filesArray != null) {
        for (File f : filesArray) {
          files.addAll(listFiles(f.toURI()));
        }
      }
    } else if (file.isFile()) {
      files.add(file);
    }
    return files;
  }

  public static String loadFileAsString(URI uri) throws IOException, URISyntaxException {
    return IOUtils.toString(getInputStream(uri), Charset.defaultCharset());
  }
}
