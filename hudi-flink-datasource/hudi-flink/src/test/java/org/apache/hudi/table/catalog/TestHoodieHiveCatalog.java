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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hudi.exception.HoodieCatalogException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for {@link HoodieHiveCatalog}.
 */
public class TestHoodieHiveCatalog {
  TableSchema schema =
      TableSchema.builder()
          .field("name", DataTypes.STRING())
          .field("age", DataTypes.INT())
          .build();
  private static HoodieHiveCatalog hoodieCatalog;
  private final ObjectPath tablePath = new ObjectPath("default", "test");

  @BeforeAll
  public static void createCatalog() {
    hoodieCatalog = TestHoodieCatalogUtils.createHiveCatalog();
    hoodieCatalog.open();
  }

  @AfterEach
  public void dropTable() throws TableNotExistException {
    hoodieCatalog.dropTable(tablePath, true);
  }

  @AfterAll
  public static void closeCatalog() {
    if (hoodieCatalog != null) {
      hoodieCatalog.close();
    }
  }

  @Test
  public void testCreateAndGetHoodieTable() throws Exception {
    Map<String, String> originOptions =
        Collections.singletonMap(FactoryUtil.CONNECTOR.key(), "hudi");
    CatalogTable table =
        new CatalogTableImpl(schema, originOptions, "hudi table");
    hoodieCatalog.createTable(tablePath, table, false);
    CatalogBaseTable table1 = hoodieCatalog.getTable(tablePath);
    assertEquals(table1.getOptions().get(CONNECTOR.key()), "hudi");
  }

  @Test
  public void testCreateNonHoodieTable() throws TableAlreadyExistException, DatabaseNotExistException {
    CatalogTable table =
        new CatalogTableImpl(schema, Collections.emptyMap(), "hudi table");
    try {
      hoodieCatalog.createTable(tablePath, table, false);
    } catch (HoodieCatalogException e) {
      assertEquals(String.format("The %s is not hoodie table", tablePath.getObjectName()), e.getMessage());
    }
  }

  @Test
  public void testAlterTable() throws Exception {
    Map<String, String> originOptions =
        Collections.singletonMap(
            FactoryUtil.CONNECTOR.key(), "hudi");
    CatalogTable originTable =
        new CatalogTableImpl(schema, originOptions, "hudi table");
    hoodieCatalog.createTable(tablePath, originTable, false);

    Table hiveTable = hoodieCatalog.getHiveTable(tablePath);
    Map<String, String> newOptions = hiveTable.getParameters();
    newOptions.put("k", "v");
    CatalogTable newTable = new CatalogTableImpl(schema, newOptions, "alter hudi table");
    hoodieCatalog.alterTable(tablePath, newTable, false);

    hiveTable = hoodieCatalog.getHiveTable(tablePath);
    assertEquals(hiveTable.getParameters().get(CONNECTOR.key()), "hudi");
    assertEquals(hiveTable.getParameters().get("k"), "v");
  }

  @Test
  public void testRenameTable() throws Exception {
    Map<String, String> originOptions =
        Collections.singletonMap(
            FactoryUtil.CONNECTOR.key(), "hudi");
    CatalogTable originTable =
        new CatalogTableImpl(schema, originOptions, "hudi table");
    hoodieCatalog.createTable(tablePath, originTable, false);

    hoodieCatalog.renameTable(tablePath, "test1", false);

    assertEquals(hoodieCatalog.getHiveTable(new ObjectPath("default", "test1")).getTableName(), "test1");

    hoodieCatalog.renameTable(new ObjectPath("default", "test1"), "test", false);
  }
}
