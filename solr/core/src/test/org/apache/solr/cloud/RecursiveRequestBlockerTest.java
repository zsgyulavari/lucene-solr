/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cloud;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * +------------+
 * | DISCLAIMER |
 * +------------+
 *
 * This test is just a stub, copied from another one.
 * The problem got fixed by https://issues.apache.org/jira/browse/SOLR-13793
 * This test should have been something like TestQueryingOnDownCollection.java
 */
public class RecursiveRequestBlockerTest extends SolrCloudTestCase {

  public static final int MAX_WAIT_TIMEOUT = 30;

  @BeforeClass
  public static void createCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  // Basically equivalent to RequestStatus.waitFor(), but doesn't delete the id from the queue
  private static RequestStatusState waitForRequestState(String id, SolrClient client, int timeout)
      throws IOException, SolrServerException, InterruptedException {
    RequestStatusState state = RequestStatusState.SUBMITTED;
    long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(MAX_WAIT_TIMEOUT);
    while (System.nanoTime() < endTime) {
      state = CollectionAdminRequest.requestStatus(id).process(client).getRequestStatus();
      if (state == RequestStatusState.COMPLETED) {
        break;
      }
      assumeTrue("Error creating collection - skipping test", state != RequestStatusState.FAILED);
      TimeUnit.SECONDS.sleep(1);
    }
    assumeTrue("Timed out creating collection - skipping test", state == RequestStatusState.COMPLETED);
    return state;
  }

  @Test
  public void testAsyncIdsMayBeDeleted() throws Exception {

    final CloudSolrClient client = cluster.getSolrClient();

    final String collection = "deletestatus";
    final String asyncId = CollectionAdminRequest.createCollection(collection, "conf1", 1, 1).processAsync(client);

    waitForRequestState(asyncId, client, MAX_WAIT_TIMEOUT);

    assertEquals(RequestStatusState.COMPLETED,
        CollectionAdminRequest.requestStatus(asyncId).process(client).getRequestStatus());

    CollectionAdminResponse rsp = CollectionAdminRequest.deleteAsyncId(asyncId).process(client);
    assertEquals("successfully removed stored response for [" + asyncId + "]", rsp.getResponse().get("status"));

    assertEquals(RequestStatusState.NOT_FOUND,
        CollectionAdminRequest.requestStatus(asyncId).process(client).getRequestStatus());

  }
}
