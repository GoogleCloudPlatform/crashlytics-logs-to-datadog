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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.solutions.bqtodatadog.testing.MockServerDispatcher;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.json.JSONException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.skyscreamer.jsonassert.ArrayValueMatcher;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.ArraySizeComparator;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.skyscreamer.jsonassert.comparator.DefaultComparator;

public class SendBigQueryTableRowToDatadogTest {

  @Rule
  public transient TestPipeline testPipeline = TestPipeline.create();

  private transient MockWebServer mockWebServer;

  @After
  public void setupMockServer() throws IOException {
    if (mockWebServer != null) {
      mockWebServer.shutdown();
    }
  }

  @Test
  public void expand_valid() throws IOException {
    MockServerDispatcher mockServerDispatcher = new MockServerDispatcher("datadog-api-key");
    HttpUrl mockServerUrl = setupMockServer(mockServerDispatcher);

    List<TableRow> testRows = generateHierarchicalRows(100);

    testPipeline
        .apply(Create.of(testRows))
        .apply(makeTestInstance(mockServerUrl));

    testPipeline.run().waitUntilFinish();

    List<String> requests = mockServerDispatcher.close();

    assertThat(requests).hasSize(1);
    assertThat(extractAllMessages(requests))
        .containsExactlyElementsIn(
            testRows.stream().map(x -> new Gson().toJson(x)).collect(toImmutableList()));
  }

  @Test
  public void expand_withGzip_valid() throws IOException {
    MockServerDispatcher mockServerDispatcher = new MockServerDispatcher("datadog-api-key", true);
    HttpUrl mockServerUrl = setupMockServer(mockServerDispatcher);

    List<TableRow> testRows = generateHierarchicalRows(100);

    testPipeline
        .apply(Create.of(testRows))
        .apply(makeTestInstance(mockServerUrl).withGzip());

    testPipeline.run().waitUntilFinish();

    List<String> requests = mockServerDispatcher.close();

    assertThat(requests).hasSize(1);
    assertThat(extractAllMessages(requests))
        .containsExactlyElementsIn(
            testRows.stream().map(x -> new Gson().toJson(x)).collect(toImmutableList()));
  }

  @Test
  public void expand_withGzip_uncompressedPayloadLessThan5Mb() throws IOException, JSONException {
    MockServerDispatcher mockServerDispatcher = new MockServerDispatcher("datadog-api-key", true);
    HttpUrl mockServerUrl = setupMockServer(mockServerDispatcher);

    List<TableRow> testRows = generateHierarchicalRows(10000);

    testPipeline
        .apply(Create.of(testRows))
        .apply(makeTestInstance(mockServerUrl).withGzip());

    testPipeline.run().waitUntilFinish();

    for (String uncompressedBody : mockServerDispatcher.close()) {
      assertThat(getByteLength(uncompressedBody)).isLessThan(5000000);
      JSONAssert
          .assertEquals("[1,1000]",
              uncompressedBody,
              new ArraySizeComparator(JSONCompareMode.LENIENT));
    }
  }

  @Test
  public void expand_validDatadogJsonKeys() throws IOException, JSONException {
    MockServerDispatcher mockServerDispatcher = new MockServerDispatcher("datadog-api-key");
    HttpUrl mockServerUrl = setupMockServer(mockServerDispatcher);

    List<TableRow> testRows = generateHierarchicalRows(1000);

    testPipeline
        .apply(Create.of(testRows))
        .apply(makeTestInstance(mockServerUrl).toBuilder()
            .datadogSource("my-custom-source-id")
            .datadogTags("my-custom-dd-tags")
            .datadogLogHostname("my-custom-dd-hostname")
            .build());

    testPipeline.run().waitUntilFinish();

    List<String> requests = mockServerDispatcher.close();

    assertThat(requests).hasSize(1);
    JSONAssert.assertEquals(
        "{r:[{ddsource:my-custom-source-id, ddtags:my-custom-dd-tags, hostname:my-custom-dd-hostname}]}",
        "{r: " + requests.get(0) + "}",
        new CustomComparator(JSONCompareMode.LENIENT,
            new Customization(
                "r",
                new ArrayValueMatcher<>(new DefaultComparator(JSONCompareMode.LENIENT)))));
  }

  @Test
  public void expand_preserveNulls_nullValuesPresent() throws IOException {
    MockServerDispatcher mockServerDispatcher = new MockServerDispatcher("datadog-api-key");
    HttpUrl mockServerUrl = setupMockServer(mockServerDispatcher);

    List<TableRow> testRows = generateHierarchicalRows(1000);

    testPipeline
        .apply(Create.of(testRows))
        .apply(
            makeTestInstance(mockServerUrl).toBuilder()
                .preserveNullValues(true)
                .build());

    testPipeline.run().waitUntilFinish();

    List<String> requests = mockServerDispatcher.close();

    assertThat(requests).hasSize(1);
    extractAllMessages(requests)
        .forEach(message -> assertThat(message).contains("\"null_col\":null"));
  }

  @Test
  public void expand_emptyData_graceful() throws IOException {
    HttpUrl mockServerUrl = setupMockServer(new MockServerDispatcher("datadog-api-key"));

    testPipeline
        .apply(Create.empty(StringDelegateCoder.of(TableRow.class)))
        .apply(makeTestInstance(mockServerUrl));

    testPipeline.run().waitUntilFinish();

    assertThat(mockWebServer.getRequestCount()).isEqualTo(0);
  }

  @Test(expected = Test.None.class /* no exception is thrown.*/)
  public void expand_error400_graceful() throws IOException {
    HttpUrl mockServerUrl = setupMockServer(new Dispatcher() {
      @Override
      public MockResponse dispatch(RecordedRequest recordedRequest) {
        return new MockResponse().setResponseCode(400).setBody("{ \"error\": \"some error\"}");
      }
    });

    List<TableRow> testRows = generateHierarchicalRows(100);

    testPipeline
        .apply(Create.of(testRows))
        .apply(makeTestInstance(mockServerUrl));

    testPipeline.run().waitUntilFinish();
  }

  private static SendBigQueryTableRowToDatadog makeTestInstance(HttpUrl mockServerUrl) {
    return SendBigQueryTableRowToDatadog.builder()
        .shardCount(1)
        .datadogApiKey("datadog-api-key")
        .endpointUrl(mockServerUrl.toString())
        .datadogTags("sample-dd-tags")
        .datadogLogHostname("test-dd-hostname")
        .datadogSource("test-dd-source")
        .build();
  }

  private static int getByteLength(String str) {
    return str.getBytes(Charsets.UTF_8).length;
  }

  private HttpUrl setupMockServer(Dispatcher dispatcher) throws IOException {
    mockWebServer = new MockWebServer();
    mockWebServer.setDispatcher(dispatcher);
    mockWebServer.start();

    return mockWebServer.url("/mock/logs/api");
  }

  private static List<String> extractAllMessages(List<String> payloads) {
    return
        payloads.stream()
            .map(SendBigQueryTableRowToDatadogTest::parseDatadogJsonPayload)
            .flatMap(List::stream)
            .map(logEntry -> logEntry.message)
            .collect(toImmutableList());
  }

  private static List<SendBigQueryTableRowToDatadog.LogEntry> parseDatadogJsonPayload(String json) {
    return new Gson()
        .fromJson(json, new TypeToken<ArrayList<SendBigQueryTableRowToDatadog.LogEntry>>() {
        }.getType());
  }

  private static ImmutableList<TableRow> generateHierarchicalRows(int numRows) {
    ImmutableList.Builder<TableRow> rowsListBuilder = ImmutableList.builder();

    Random random = new Random();

    for (int row = 0; row < numRows; row++) {

      TableRow tableRow =
          new TableRow()
              .set("string_col", String.format("%s", UUID.randomUUID().toString()))
              .set("null_col", null)
              .set("numeric_col", random.nextDouble())
              .set("nested_col",
                  new TableRow()
                      .set("nest1", random.nextInt())
                      .set("repeated_col", ImmutableList.of(
                          new TableRow().set("repeat_nested_col1", "1")
                              .set("repeat_nested_col2", "2"),
                          new TableRow().set("repeat_nested_col1", "1.1")
                              .set("repeat_nested_col2", "2.1")
                      )));

      rowsListBuilder.add(tableRow);
    }

    return rowsListBuilder.build();
  }
}