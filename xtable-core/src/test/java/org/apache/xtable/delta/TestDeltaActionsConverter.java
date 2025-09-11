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

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor;

import scala.Option;

import org.apache.xtable.model.storage.InternalDeletionVector;

class TestDeltaActionsConverter {

  private final String basePath = "https://contaner.blob.core.windows.net/tablepath";
  private final int size = 372;
  private final long time = 376;
  private final boolean dataChange = true;
  private final String stats = "";
  private final int cardinality = 42;
  private final int offset = 674;

  @Test
  void extractMissingDeletionVector() {
    DeltaActionsConverter actionsConverter = DeltaActionsConverter.getInstance();

    String filePath = basePath + "file_path";
    Snapshot snapshot = Mockito.mock(Snapshot.class);

    DeletionVectorDescriptor deletionVector = null;
    AddFile addFileAction =
        new AddFile(filePath, null, size, time, dataChange, stats, null, deletionVector);
    InternalDeletionVector internalDeletionVector =
        actionsConverter.extractDeletionVector(snapshot, addFileAction);
    Assertions.assertNull(internalDeletionVector);
  }

  @Test
  void extractDeletionVectorInFileAbsolutePath() {
    DeltaActionsConverter actionsConverter = spy(DeltaActionsConverter.getInstance());

    String dataFilePath = "data_file";
    String deleteFilePath = "https://container.blob.core.windows.net/tablepath/delete_path";
    Snapshot snapshot = Mockito.mock(Snapshot.class);

    DeletionVectorDescriptor deletionVector =
        DeletionVectorDescriptor.onDiskWithAbsolutePath(
            deleteFilePath, size, cardinality, Option.apply(offset), Option.empty());

    AddFile addFileAction =
        new AddFile(dataFilePath, null, size, time, dataChange, stats, null, deletionVector);

    Configuration conf = new Configuration();
    DeltaLog deltaLog = Mockito.mock(DeltaLog.class);
    when(snapshot.deltaLog()).thenReturn(deltaLog);
    when(deltaLog.dataPath()).thenReturn(new Path(basePath));
    when(deltaLog.newDeltaHadoopConf()).thenReturn(conf);

    long[] ordinals = {45, 78, 98};
    Mockito.doReturn(ordinals)
        .when(actionsConverter)
        .parseOrdinalFile(conf, new Path(deleteFilePath), size, offset);

    InternalDeletionVector internalDeletionVector =
        actionsConverter.extractDeletionVector(snapshot, addFileAction);
    Assertions.assertNotNull(internalDeletionVector);
    Assertions.assertEquals(basePath + "/" + dataFilePath, internalDeletionVector.dataFilePath());
    Assertions.assertEquals(deleteFilePath, internalDeletionVector.getPhysicalPath());
    Assertions.assertEquals(offset, internalDeletionVector.offset());
    Assertions.assertEquals(cardinality, internalDeletionVector.getRecordCount());
    Assertions.assertEquals(size, internalDeletionVector.getFileSizeBytes());
    Assertions.assertNull(internalDeletionVector.binaryRepresentation());
  }

  @Test
  void extractDeletionVector() throws URISyntaxException {
    DeltaActionsConverter actionsConverter = DeltaActionsConverter.getInstance();

    int size = 123;
    long time = 234L;
    boolean dataChange = true;
    String stats = "";
    String filePath = "https://container.blob.core.windows.net/tablepath/file_path";
    Snapshot snapshot = Mockito.mock(Snapshot.class);
    DeltaLog deltaLog = Mockito.mock(DeltaLog.class);

    DeletionVectorDescriptor deletionVector = null;
    AddFile addFileAction =
        new AddFile(filePath, null, size, time, dataChange, stats, null, deletionVector);
    Assertions.assertNull(actionsConverter.extractDeletionVector(snapshot, addFileAction));

    deletionVector =
        DeletionVectorDescriptor.onDiskWithAbsolutePath(
            filePath, size, 42, Option.empty(), Option.empty());

    addFileAction =
        new AddFile(filePath, null, size, time, dataChange, stats, null, deletionVector);

    Mockito.when(snapshot.deltaLog()).thenReturn(deltaLog);
    Mockito.when(deltaLog.dataPath())
        .thenReturn(new Path("https://container.blob.core.windows.net/tablepath"));
    Assertions.assertEquals(
        filePath, actionsConverter.extractDeletionVector(snapshot, addFileAction));
  }
}
