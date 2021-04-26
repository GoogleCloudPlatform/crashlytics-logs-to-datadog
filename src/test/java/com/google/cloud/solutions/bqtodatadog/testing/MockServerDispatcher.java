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

package com.google.cloud.solutions.bqtodatadog.testing;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.truth.Truth.assertThat;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import org.jetbrains.annotations.NotNull;

public class MockServerDispatcher extends Dispatcher {

  private final String expectedDdApiKey;
  private final boolean isGzip;
  private final ImmutableList.Builder<String> requestsListBuilder;
  private boolean closed;

  public MockServerDispatcher(String expectedDdApiKey, boolean isGzip) {
    this.expectedDdApiKey = expectedDdApiKey;
    this.isGzip = isGzip;
    this.requestsListBuilder = ImmutableList.builder();
    this.closed = false;
  }

  public MockServerDispatcher(String expectedDdApiKey) {
    this(expectedDdApiKey, /*assertGzip=*/ false);
  }

  public ImmutableList<String> close() {
    return requestsListBuilder.build();
  }

  @NotNull
  @Override
  public MockResponse dispatch(@NotNull RecordedRequest recordedRequest) {
    checkState(!closed, "dispatcher should not be closed.");

    assertThat(recordedRequest.getHeaders().get("DD-API-KEY")).isEqualTo(expectedDdApiKey);
    assertThat(recordedRequest.getHeader(HttpHeaders.CONTENT_TYPE))
        .startsWith(MediaType.JSON_UTF_8.type());

    try {
      boolean requestGzip =
          "gzip".equals(recordedRequest.getHeaders().get(HttpHeaders.CONTENT_ENCODING));

      assertThat(requestGzip).isEqualTo(isGzip);

      RequestBodySource source = new RequestBodySource(requestGzip, recordedRequest);
      requestsListBuilder.add(source.asCharSource(Charsets.UTF_8).read());

    } catch (IOException ioException) {
      return new MockResponse().setResponseCode(400)
          .setBody("error: " + ioException.getMessage());
    }

    return new MockResponse().setBody("{}").setResponseCode(200);
  }

  private class RequestBodySource extends ByteSource {

    private final boolean isGzip;
    private final RecordedRequest recordedRequest;

    public RequestBodySource(boolean isGzip, RecordedRequest recordedRequest) {
      this.isGzip = isGzip;
      this.recordedRequest = recordedRequest;
    }

    @Override
    public InputStream openStream() throws IOException {
      try {
        InputStream requestBodyStream = recordedRequest.getBody().inputStream();
        return (isGzip) ? new GZIPInputStream(requestBodyStream) : requestBodyStream;
      } catch (NullPointerException npe) {
        return new ByteArrayInputStream(new byte[0]);
      }
    }
  }
}
