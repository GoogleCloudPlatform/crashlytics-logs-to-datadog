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

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.bqtodatadog.accumulator.GroupByBatchSize;
import com.google.cloud.solutions.bqtodatadog.accumulator.GroupByBatchSize.BatchAccumulator;
import com.google.cloud.solutions.bqtodatadog.accumulator.GroupByBatchSize.BatchAccumulator.BatchAccumulatorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPOutputStream;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;


/** Send BigQuery TableRow to Datadog as log entry as JSON. */
@AutoValue
public abstract class SendBigQueryTableRowToDatadog extends
    PTransform<PCollection<TableRow>, PCollection<Void>> {

  abstract String endpointUrl();

  abstract String datadogApiKey();

  abstract Integer shardCount();

  abstract Boolean enableGzip();

  abstract String datadogSource();

  abstract String datadogTags();

  abstract String datadogLogHostname();

  abstract boolean preserveNullValues();

  public SendBigQueryTableRowToDatadog withGzip() {
    return toBuilder().enableGzip(true).build();
  }


  @Override
  public PCollection<Void> expand(PCollection<TableRow> input) {
    return input
        .apply("MakeLogEntry", MapElements.via(new MakeLogEntryFromTableRowFn()))
        .setCoder(StringUtf8Coder.of())
        .apply("ShardTheData", WithKeys.of((String x) -> new Random().nextInt(shardCount())))
        .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()))
        .apply("BatchBySize", GroupByBatchSize.withAccumulator(new StringsAccumulatorFactory()))
        .setCoder(ListCoder.of(StringUtf8Coder.of()))
        .apply("SendToDatadog",
            ParDo.of(
                SendAsDatadogLogFn.builder()
                    .datadogApiKey(datadogApiKey())
                    .endpointUrl(endpointUrl())
                    .enableGzip(enableGzip())
                    .build()));
  }

  public static Builder builder() {
    return new AutoValue_SendBigQueryTableRowToDatadog
        .Builder()
        .enableGzip(false)
        .preserveNullValues(false);
  }

  abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder endpointUrl(String value);

    public abstract Builder datadogApiKey(String value);

    public abstract Builder shardCount(Integer shardCount);

    public abstract Builder enableGzip(Boolean enableGzip);

    public abstract Builder datadogSource(String datadogSource);

    public abstract Builder datadogTags(String datadogTags);

    public abstract Builder datadogLogHostname(String datadogLogHostname);

    public abstract Builder preserveNullValues(boolean preserveNullValues);

    public abstract SendBigQueryTableRowToDatadog build();
  }


  @AutoValue
  abstract static class SendAsDatadogLogFn extends DoFn<List<String>, Void> {

    private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

    abstract String endpointUrl();

    abstract String datadogApiKey();

    abstract boolean enableGzip();

    @ProcessElement
    public void sendBatchToServer(@Element List<String> batch) {
      if (batch == null || batch.isEmpty()) {
        logger.atInfo().log("Skipping empty batch");
        return;
      }

      logger.atInfo().log("Sending batch with %s rows", batch.size());

      OkHttpClient client = new OkHttpClient();
      DatadogRequestMaker requestMaker = new DatadogRequestMaker(batch);

      try (Response response = client.newCall(requestMaker.makeRequest()).execute()) {
        if (response.code() != 200) {

          ResponseBody body = response.body();

          logger.atSevere().log("Datadog Logs API error:%n%s", (body == null) ? "" : body.string());
        }
      } catch (IOException ioException) {
        logger.atSevere().withCause(ioException).log("Error sending logs to Datadog");
      }
    }

    private class DatadogRequestMaker {

      private final ImmutableList<String> logEntries;
      private final byte[] rawBody;
      private boolean gzipFailed;

      public DatadogRequestMaker(List<String> logEntries) {
        this.logEntries = ImmutableList.copyOf(logEntries);
        this.rawBody = logEntries.toString().getBytes(StandardCharsets.UTF_8);
        this.gzipFailed = false;
      }

      public Request makeRequest() {
        Request.Builder requestBuilder = new Request.Builder()
            .post(makeRequestBody())
            .url(endpointUrl())
            .addHeader("DD-API-KEY", datadogApiKey());

        if (enableGzip() && !gzipFailed) {
          requestBuilder = requestBuilder.header("Content-Encoding", "gzip");
        }

        return requestBuilder.build();
      }

      private RequestBody makeRequestBody() {
        return RequestBody.create(enableGzip() ? gzipedLogEntries() : rawBody,
            MediaType.get("application/json; charset=utf-8"));
      }

      private byte[] gzipedLogEntries() {

        try (ByteArrayOutputStream gzippedBytesStream = new ByteArrayOutputStream()) {
          OutputStream zipper = new GZIPOutputStream(gzippedBytesStream);
          zipper.write(rawBody);
          zipper.close();

          return gzippedBytesStream.toByteArray();
        } catch (IOException ioException) {
          logger.atSevere().withCause(ioException).log("error gziping request.");
          gzipFailed = true;
        }

        return logEntries.toString().getBytes(StandardCharsets.UTF_8);
      }
    }

    public static Builder builder() {
      return new AutoValue_SendBigQueryTableRowToDatadog_SendAsDatadogLogFn.Builder();
    }


    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder endpointUrl(String value);

      public abstract Builder datadogApiKey(String value);

      public abstract Builder enableGzip(boolean enableGzip);

      public abstract SendAsDatadogLogFn build();
    }
  }

  private final class MakeLogEntryFromTableRowFn extends SimpleFunction<TableRow, String> {

    @Override
    public String apply(TableRow row) {
      Gson gson = makeGson();

      return gson
          .toJson(
              new LogEntry(datadogSource(), datadogTags(), datadogLogHostname(), gson.toJson(row)));
    }

    private Gson makeGson() {
      GsonBuilder gsonBuilder = new GsonBuilder();

      if (preserveNullValues()) {
        gsonBuilder.serializeNulls();
      }

      return gsonBuilder.create();
    }
  }


  private final class StringsAccumulatorFactory implements
      BatchAccumulatorFactory<String, List<String>> {

    @Override
    public BatchAccumulator<String, List<String>> newAccumulator() {
      return new StringsAccumulator();
    }
  }

  private final class StringsAccumulator implements BatchAccumulator<String, List<String>> {

    private static final int MAX_BATCH_SIZE = 5000000; //~5MB
    private static final int MAX_MESSAGE_COUNT = 1000;

    private final LinkedList<String> accumulatedMessages = Lists.newLinkedList();
    private int accumulatedBytes = 0;


    @Override
    public boolean addElement(String element) {
      int elementByteSize = element.getBytes(StandardCharsets.UTF_8).length;

      if (accumulatedBytes + elementByteSize > MAX_BATCH_SIZE
          || accumulatedMessages.size() == MAX_MESSAGE_COUNT) {
        return false;
      }

      accumulatedMessages.add(element);
      accumulatedBytes += elementByteSize;

      return true;
    }

    @Override
    public Batch<List<String>> makeBatch() {

      return new Batch<List<String>>() {

        private final ImmutableList<String> messages = ImmutableList.copyOf(accumulatedMessages);
        private final int accumulatedDataSize = accumulatedBytes;

        @Override
        public List<String> get() {
          return messages;
        }

        @Override
        public int elementsCount() {
          return messages.size();
        }

        @Override
        public int serializedSize() {
          return accumulatedDataSize;
        }

        @Override
        public String report() {
          return String.format("Elements:%s%nBytes:%s", accumulatedMessages, accumulatedDataSize);
        }
      };
    }
  }


  /**
   * Model class for Datadog log.
   */
  static class LogEntry {

    public final String ddsource;
    public final String ddtags;
    public final String hostname;
    public final String message;

    public LogEntry(String ddsource, String ddtags, String hostname, String message) {
      this.ddsource = ddsource;
      this.ddtags = ddtags;
      this.hostname = hostname;
      this.message = message;
    }
  }
}
