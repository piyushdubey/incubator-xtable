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
 
package org.apache.xtable.hms;

import java.util.Map;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import org.apache.hudi.hive.MultiPartKeysValueExtractor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@Getter
@EqualsAndHashCode
@ToString
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class HMSCatalogConfig {

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty("externalCatalog.hms.serverUrl")
  private final String serverUrl;

  @JsonProperty("externalCatalog.hms.schema_string_length_thresh")
  private final int schemaLengthThreshold = 4000;

  @JsonProperty("externalCatalog.hms.partition_extractor_class")
  private final String partitionExtractorClass = MultiPartKeysValueExtractor.class.getName();

  @JsonProperty("externalCatalog.hms.max_partitions_per_request")
  private final int maxPartitionsPerRequest = 1000;

  protected static HMSCatalogConfig of(Map<String, String> properties) {
    try {
      return OBJECT_MAPPER.readValue(
          OBJECT_MAPPER.writeValueAsString(properties), HMSCatalogConfig.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
