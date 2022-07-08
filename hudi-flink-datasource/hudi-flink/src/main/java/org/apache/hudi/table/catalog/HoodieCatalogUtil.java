/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.catalog;

import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;
import static org.apache.hudi.table.catalog.HoodieCatalogFactoryOptions.HIVE_SITE_FILE;

/**
 * Utilities for Hoodie Catalog.
 */
public class HoodieCatalogUtil {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieCatalogUtil.class);

  /**
   * Returns a new hiveConfig.
   *
   * @param hiveConfDir Hive conf directory path.
   * @param hadoopConfDir Hadoop conf directory path.
   * @return A HiveConf instance.
   */
  public static HiveConf createHiveConf(@Nullable String hiveConfDir, @Nullable String hadoopConfDir) {
    // create HiveConf from hadoop configuration with hadoop conf directory configured.
    Configuration hadoopConf = null;
    if (isNullOrWhitespaceOnly(hadoopConfDir)) {
      for (String possibleHadoopConfPath :
          HadoopUtils.possibleHadoopConfPaths(
              new org.apache.flink.configuration.Configuration())) {
        hadoopConf = getHadoopConfiguration(possibleHadoopConfPath);
        if (hadoopConf != null) {
          break;
        }
      }
    } else {
      hadoopConf = getHadoopConfiguration(hadoopConfDir);
      if (hadoopConf == null) {
        String possiableUsedConfFiles =
            "core-site.xml | hdfs-site.xml | yarn-site.xml | mapred-site.xml";
        throw new CatalogException(
            "Failed to load the hadoop conf from specified path:" + hadoopConfDir,
            new FileNotFoundException(
                "Please check the path none of the conf files ("
                    + possiableUsedConfFiles
                    + ") exist in the folder."));
      }
    }
    if (hadoopConf == null) {
      hadoopConf = new Configuration();
    }
    // ignore all the static conf file URLs that HiveConf may have set
    HiveConf.setHiveSiteLocation(null);
    HiveConf.setLoadMetastoreConfig(false);
    HiveConf.setLoadHiveServer2Config(false);
    HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);

    LOG.info("Setting hive conf dir as {}", hiveConfDir);

    if (hiveConfDir != null) {
      Path hiveSite = new Path(hiveConfDir, HIVE_SITE_FILE);
      if (!hiveSite.toUri().isAbsolute()) {
        // treat relative URI as local file to be compatible with previous behavior
        hiveSite = new Path(new File(hiveSite.toString()).toURI());
      }
      try (InputStream inputStream = hiveSite.getFileSystem(hadoopConf).open(hiveSite)) {
        hiveConf.addResource(inputStream, hiveSite.toString());
        // trigger a read from the conf so that the input stream is read
        isEmbeddedMetastore(hiveConf);
      } catch (IOException e) {
        throw new CatalogException(
            "Failed to load hive-site.xml from specified path:" + hiveSite, e);
      }
    } else {
      // user doesn't provide hive conf dir, we try to find it in classpath
      URL hiveSite =
          Thread.currentThread().getContextClassLoader().getResource(HIVE_SITE_FILE);
      if (hiveSite != null) {
        LOG.info("Found {} in classpath: {}", HIVE_SITE_FILE, hiveSite);
        hiveConf.addResource(hiveSite);
      }
    }
    return hiveConf;
  }

  /**
   * Check whether the hive.metastore.uris is empty
   */
  public static boolean isEmbeddedMetastore(HiveConf hiveConf) {
    return isNullOrWhitespaceOnly(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
  }

  /**
   * Returns a new Hadoop Configuration object using the path to the hadoop conf configured.
   *
   * @param hadoopConfDir Hadoop conf directory path.
   * @return A Hadoop configuration instance.
   */
  public static Configuration getHadoopConfiguration(String hadoopConfDir) {
    if (new File(hadoopConfDir).exists()) {
      List<File> possiableConfFiles = new ArrayList<File>();
      File coreSite = new File(hadoopConfDir, "core-site.xml");
      if (coreSite.exists()) {
        possiableConfFiles.add(coreSite);
      }
      File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
      if (hdfsSite.exists()) {
        possiableConfFiles.add(hdfsSite);
      }
      File yarnSite = new File(hadoopConfDir, "yarn-site.xml");
      if (yarnSite.exists()) {
        possiableConfFiles.add(yarnSite);
      }
      // Add mapred-site.xml. We need to read configurations like compression codec.
      File mapredSite = new File(hadoopConfDir, "mapred-site.xml");
      if (mapredSite.exists()) {
        possiableConfFiles.add(mapredSite);
      }
      if (possiableConfFiles.isEmpty()) {
        return null;
      } else {
        Configuration hadoopConfiguration = new Configuration();
        for (File confFile : possiableConfFiles) {
          hadoopConfiguration.addResource(new Path(confFile.getAbsolutePath()));
        }
        return hadoopConfiguration;
      }
    }
    return null;
  }
}
