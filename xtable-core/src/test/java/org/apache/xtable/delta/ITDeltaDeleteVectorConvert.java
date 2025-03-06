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
 
package org.apache.xtable.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor;

import scala.Option;

import org.apache.xtable.GenericTable;
import org.apache.xtable.TestSparkDeltaTable;
import org.apache.xtable.ValidationTestHelper;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.storage.InternalDeletionVector;
import org.apache.xtable.model.storage.TableFormat;

public class ITDeltaDeleteVectorConvert {
  @TempDir private static Path tempDir;
  private static SparkSession sparkSession;

  private DeltaConversionSourceProvider conversionSourceProvider;
  private TestSparkDeltaTable testSparkDeltaTable;

  @BeforeAll
  public static void setupOnce() {
    sparkSession =
        SparkSession.builder()
            .appName("TestDeltaTable")
            .master("local[4]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.serializer", KryoSerializer.class.getName())
            .getOrCreate();
  }

  @AfterAll
  public static void teardown() {
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  @BeforeEach
  void setUp() {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");

    conversionSourceProvider = new DeltaConversionSourceProvider();
    conversionSourceProvider.init(hadoopConf);
  }

  private static class TableState {
    Map<String, AddFile> activeFiles;
    List<Row> rowsToDelete;

    TableState(Map<String, AddFile> activeFiles) {
      this(activeFiles, Collections.emptyList());
    }

    TableState(Map<String, AddFile> activeFiles, List<Row> rowsToDelete) {
      this.activeFiles = activeFiles;
      this.rowsToDelete = rowsToDelete;
    }
  }

  @Test
  public void testInsertsUpsertsAndDeletes() {
    String tableName = GenericTable.getTableName();
    testSparkDeltaTable = new TestSparkDeltaTable(tableName, tempDir, sparkSession, null, false);

    // enable deletion vectors for the test table
    testSparkDeltaTable
        .getSparkSession()
        .sql(
            "ALTER TABLE "
                + tableName
                + " SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)");

    List<TableState> testTableStates = new ArrayList<>();
    List<TableChange> allTableChanges = new ArrayList<>();
    List<Row> rows = testSparkDeltaTable.insertRows(50);
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    Map<String, AddFile> tableFiles = collectActiveFilesAfterCommit(testSparkDeltaTable);
    testTableStates.add(new TableState(tableFiles, Collections.emptyList()));

    List<Row> rows1 = testSparkDeltaTable.insertRows(50);
    tableFiles = collectActiveFilesAfterCommit(testSparkDeltaTable);
    testTableStates.add(new TableState(tableFiles));
    validateDeletedRecordCount(testSparkDeltaTable.getDeltaLog(), 0, 0);
    assertEquals(100L, testSparkDeltaTable.getNumRows());

    // upsert does not create delete vectors
    testSparkDeltaTable.upsertRows(rows.subList(0, 20));
    tableFiles = collectActiveFilesAfterCommit(testSparkDeltaTable);
    testTableStates.add(new TableState(tableFiles));
    validateDeletedRecordCount(testSparkDeltaTable.getDeltaLog(), 0, 0);
    assertEquals(100L, testSparkDeltaTable.getNumRows());

    testSparkDeltaTable.insertRows(50);
    tableFiles = collectActiveFilesAfterCommit(testSparkDeltaTable);
    testTableStates.add(new TableState(tableFiles));
    validateDeletedRecordCount(testSparkDeltaTable.getDeltaLog(), 0, 0);
    assertEquals(150L, testSparkDeltaTable.getNumRows());

    // delete a few rows with gaps in ids
    List<Row> rowsToDelete =
        rows1.subList(0, 10).stream()
            .filter(row -> (row.get(0).hashCode() % 2) == 0)
            .collect(Collectors.toList());
    rowsToDelete.addAll(rows.subList(35, 45));
    testSparkDeltaTable.deleteRows(rowsToDelete);
    tableFiles = collectActiveFilesAfterCommit(testSparkDeltaTable);
    testTableStates.add(new TableState(tableFiles, rowsToDelete));
    validateDeletedRecordCount(testSparkDeltaTable.getDeltaLog(), 2, 15);
    assertEquals(135L, testSparkDeltaTable.getNumRows());

    testSparkDeltaTable.insertRows(50);
    tableFiles = collectActiveFilesAfterCommit(testSparkDeltaTable);
    testTableStates.add(new TableState(tableFiles));
    validateDeletedRecordCount(testSparkDeltaTable.getDeltaLog(), 2, 15);
    assertEquals(185L, testSparkDeltaTable.getNumRows());

    // delete a few rows from a file which already has a deletion vector, this should generate a
    // merged deletion vector file. Some rows were already deleted in the previous delete step.
    // This deletion step intentionally deletes the same rows again to test the merge.
    rowsToDelete = rows1.subList(5, 15);
    testSparkDeltaTable.deleteRows(rowsToDelete);
    tableFiles = collectActiveFilesAfterCommit(testSparkDeltaTable);
    testTableStates.add(new TableState(tableFiles, rowsToDelete));
    validateDeletedRecordCount(testSparkDeltaTable.getDeltaLog(), 2, 22);
    assertEquals(178L, testSparkDeltaTable.getNumRows());

    testSparkDeltaTable.insertRows(50);
    tableFiles = collectActiveFilesAfterCommit(testSparkDeltaTable);
    testTableStates.add(new TableState(tableFiles));
    validateDeletedRecordCount(testSparkDeltaTable.getDeltaLog(), 2, 22);
    assertEquals(228L, testSparkDeltaTable.getNumRows());

    String tableBasePath = testSparkDeltaTable.getBasePath();
    SourceTable tableConfig =
        SourceTable.builder()
            .name(testSparkDeltaTable.getTableName())
            .basePath(tableBasePath)
            .formatName(TableFormat.DELTA)
            .build();
    DeltaConversionSource conversionSource =
        conversionSourceProvider.getConversionSourceInstance(tableConfig);
    InternalSnapshot internalSnapshot = conversionSource.getCurrentSnapshot();

    //    validateDeltaPartitioning(internalSnapshot);
    List<String> activeDataFilePaths =
        new ArrayList<>(testTableStates.get(testTableStates.size() - 1).activeFiles.keySet());
    ValidationTestHelper.validateSnapshot(internalSnapshot, activeDataFilePaths);

    // Get changes in incremental format.
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
            .build();
    CommitsBacklog<Long> commitsBacklog =
        conversionSource.getCommitsBacklog(instantsForIncrementalSync);
    for (Long version : commitsBacklog.getCommitsToProcess()) {
      TableChange tableChange = conversionSource.getTableChangeForCommit(version);
      allTableChanges.add(tableChange);
    }

    List<List<String>> allActiveDataFilePaths =
        testTableStates.stream()
            .map(s -> s.activeFiles)
            .map(Map::keySet)
            .map(ArrayList::new)
            .collect(Collectors.toList());
    ValidationTestHelper.validateTableChanges(allActiveDataFilePaths, allTableChanges);

    validateDeletionInfo(testTableStates, allTableChanges);
  }

  // collects active files in the current snapshot as a map and adds it to the list
  private Map<String, AddFile> collectActiveFilesAfterCommit(
      TestSparkDeltaTable testSparkDeltaTable) {
    Map<String, AddFile> allFiles =
        testSparkDeltaTable.getAllActiveFilesInfo().stream()
            .collect(
                Collectors.toMap(
                    file -> getAddFileAbsolutePath(file, testSparkDeltaTable.getBasePath()),
                    file -> file));
    return allFiles;
  }

  private void validateDeletionInfo(
      List<TableState> testTableStates, List<TableChange> allTableChanges) {
    if (allTableChanges.isEmpty() && testTableStates.size() <= 1) {
      return;
    }

    assertEquals(
        allTableChanges.size(),
        testTableStates.size() - 1,
        "Number of table changes should be equal to number of commits - 1");

    for (int i = 0; i < allTableChanges.size() - 1; i++) {
      Map<String, AddFile> activeFileAfterCommit = testTableStates.get(i + 1).activeFiles;
      Map<String, AddFile> activeFileBeforeCommit = testTableStates.get(i).activeFiles;

      Map<String, AddFile> activeFilesWithUpdatedDeleteInfo =
          activeFileAfterCommit.entrySet().stream()
              .filter(e -> e.getValue().deletionVector() != null)
              .filter(
                  entry -> {
                    if (activeFileBeforeCommit.get(entry.getKey()) == null) {
                      return true;
                    }
                    if (activeFileBeforeCommit.get(entry.getKey()).deletionVector() == null) {
                      return true;
                    }
                    DeletionVectorDescriptor deletionVectorDescriptor =
                        activeFileBeforeCommit.get(entry.getKey()).deletionVector();
                    return !deletionVectorDescriptor.equals(entry.getValue().deletionVector());
                  })
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      if (activeFilesWithUpdatedDeleteInfo.isEmpty()) {
        continue;
      }

      // validate all new delete vectors are correctly detected
      validateDeletionInfoForCommit(
          testTableStates.get(i + 1), activeFilesWithUpdatedDeleteInfo, allTableChanges.get(i));
    }
  }

  private void validateDeletionInfoForCommit(
      TableState tableState,
      Map<String, AddFile> activeFilesAfterCommit,
      TableChange changeDetectedForCommit) {
    Map<String, InternalDeletionVector> detectedDeleteInfos =
        changeDetectedForCommit.getFilesDiff().getFilesAdded().stream()
            .filter(file -> file instanceof InternalDeletionVector)
            .map(file -> (InternalDeletionVector) file)
            .collect(Collectors.toMap(InternalDeletionVector::dataFilePath, file -> file));

    Map<String, AddFile> filesWithDeleteVectors =
        activeFilesAfterCommit.entrySet().stream()
            .filter(file -> file.getValue().deletionVector() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    assertEquals(filesWithDeleteVectors.size(), detectedDeleteInfos.size());

    for (Map.Entry<String, AddFile> fileWithDeleteVector : filesWithDeleteVectors.entrySet()) {
      InternalDeletionVector deleteInfo = detectedDeleteInfos.get(fileWithDeleteVector.getKey());
      assertNotNull(deleteInfo);
      DeletionVectorDescriptor deletionVectorDescriptor =
          fileWithDeleteVector.getValue().deletionVector();
      assertEquals(deletionVectorDescriptor.cardinality(), deleteInfo.getRecordCount());
      assertEquals(deletionVectorDescriptor.sizeInBytes(), deleteInfo.getFileSizeBytes());
      assertEquals(deletionVectorDescriptor.offset().get(), deleteInfo.offset());

      String deletionFilePath =
          deletionVectorDescriptor
              .absolutePath(new org.apache.hadoop.fs.Path(testSparkDeltaTable.getBasePath()))
              .toString();
      assertEquals(deletionFilePath, deleteInfo.getPhysicalPath());

      Iterator<Long> iterator = deleteInfo.ordinalsIterator();
      List<Long> deletes = new ArrayList<>();
      iterator.forEachRemaining(deletes::add);
      assertEquals(deletes.size(), deleteInfo.getRecordCount());
    }
  }

  private static String getAddFileAbsolutePath(AddFile file, String tableBasePath) {
    String filePath = file.path();
    if (filePath.startsWith(tableBasePath)) {
      return filePath;
    }
    tableBasePath = tableBasePath.replaceAll("/$", "");
    return String.join("/", tableBasePath, file.path());
  }

  private void validateDeletedRecordCount(
      DeltaLog deltaLog, int deleteVectorFileCount, int deletionRecordCount) {
    List<AddFile> allFiles =
        deltaLog
            .getSnapshotAt(deltaLog.snapshot().version(), Option.empty())
            .allFiles()
            .collectAsList();
    List<AddFile> filesWithDeletionVectors =
        allFiles.stream().filter(f -> f.deletionVector() != null).collect(Collectors.toList());

    assertEquals(deleteVectorFileCount, filesWithDeletionVectors.size());
    assertEquals(
        deletionRecordCount,
        filesWithDeletionVectors.stream()
            .collect(Collectors.summarizingLong(AddFile::numDeletedRecords))
            .getSum());
  }
}
