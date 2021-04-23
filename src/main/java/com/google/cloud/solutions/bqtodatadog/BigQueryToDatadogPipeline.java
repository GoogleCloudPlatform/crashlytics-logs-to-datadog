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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

@AutoValue
public abstract class BigQueryToDatadogPipeline {

  abstract Pipeline pipeline();

  abstract BigQueryToDatadogPipelineOptions options();

  public static void main(String[] args) {
    PipelineOptionsFactory.register(BigQueryToDatadogPipelineOptions.class);

    BigQueryToDatadogPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(BigQueryToDatadogPipelineOptions.class);

    BigQueryToDatadogPipeline.withOptions(options).execute();
  }

  public PipelineResult execute() {
    return buildPipeline().run();
  }

  /**
   * Constructs the pipeline.
   */
  private Pipeline buildPipeline() {
    pipeline()
        .apply("ReadBigQueryTable", bigqueryReader())
        .apply(
            "BatchDataAndSendToDatadog",
            SendBigQueryTableRowToDatadog.builder()
                .datadogApiKey(options().getDatadogApiKey())
                .endpointUrl(options().getDatadogEndpoint())
                .shardCount(options().getShardCount())
                .enableGzip(true)
                .preserveNullValues(options().getPreserveNullValues())
                .datadogSource(options().getDatadogSource())
                .datadogTags(options().getDatadogTags())
                .datadogLogHostname(options().getDatadogLogHostname())
                .build());

    return pipeline();
  }

  /**
   * Returns the BigQuery reader for Table or output of SQL query.
   */
  private BigQueryIO.TypedRead<TableRow> bigqueryReader() {
    checkArgument(
        (options().getBigQuerySqlQuery() == null) ^ (options().getSourceBigQueryTableId() == null),
        "Only one of bigQuerySqlQuery or sourceBigQueryTableId should be present found:%nbigQuerySqlQuery=%s%nsourceBigQueryTableId=%s",
        options().getBigQuerySqlQuery(), options().getSourceBigQueryTableId());

    if (options().getSourceBigQueryTableId() != null) {
      return BigQueryIO.readTableRows().from(options().getSourceBigQueryTableId());
    }

    return BigQueryIO.readTableRows().fromQuery(options().getBigQuerySqlQuery()).usingStandardSql();
  }

  private static BigQueryToDatadogPipeline withOptions(BigQueryToDatadogPipelineOptions options) {
    checkNotNull(options);

    return
        new AutoValue_BigQueryToDatadogPipeline.Builder()
            .setOptions(options)
            .setPipeline(Pipeline.create(options))
            .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setPipeline(Pipeline value);

    public abstract Builder setOptions(BigQueryToDatadogPipelineOptions value);

    public abstract BigQueryToDatadogPipeline build();
  }
}
