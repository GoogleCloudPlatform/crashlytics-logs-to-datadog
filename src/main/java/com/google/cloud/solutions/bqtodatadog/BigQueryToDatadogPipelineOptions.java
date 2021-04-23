/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.solutions.bqtodatadog;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

public interface BigQueryToDatadogPipelineOptions extends GcpOptions {

  @Validation.Required
  String getDatadogApiKey();

  void setDatadogApiKey(String datadogApiKey);

  @Default.String("https://http-intake.logs.datadoghq.com/v1/input")
  String getDatadogEndpoint();

  void setDatadogEndpoint(String datadogEndpoint);

  @Default.String("crashlytics-bigquery")
  String getDatadogSource();

  void setDatadogSource(String datadogSource);

  @Default.String("user:crashlytics-pipeline")
  String getDatadogTags();

  void setDatadogTags(String datadogTags);

  @Default.String("crashlytics")
  String getDatadogLogHostname();

  void setDatadogLogHostname(String datadogLogHostname);

  String getSourceBigQueryTableId();

  void setSourceBigQueryTableId(String sourceBigQueryTableId);

  String getBigQuerySqlQuery();

  void setBigQuerySqlQuery(String bigQuerySqlQuery);

  @Default.Boolean(false)
  Boolean getPreserveNullValues();

  void setPreserveNullValues(Boolean preserveNullValues);

  @Default.Integer(10)
  Integer getShardCount();

  void setShardCount(Integer shardCount);
}
