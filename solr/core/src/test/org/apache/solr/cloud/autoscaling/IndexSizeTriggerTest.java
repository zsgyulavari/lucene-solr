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

package org.apache.solr.cloud.autoscaling;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.autoscaling.sim.SimCloudManager;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;

/**
 *
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class IndexSizeTriggerTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static SolrCloudManager cloudManager;
  private static SolrClient solrClient;
  private static TimeSource timeSource;
  private static SolrResourceLoader loader;

  private AutoScaling.TriggerEventProcessor noFirstRunProcessor = event -> {
    fail("Did not expect the processor to fire on first run! event=" + event);
    return true;
  };
  private static final long WAIT_FOR_DELTA_NANOS = TimeUnit.MILLISECONDS.toNanos(2);

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    if (random().nextBoolean() && false) {
      cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
      solrClient = cluster.getSolrClient();
      loader = cluster.getJettySolrRunner(0).getCoreContainer().getResourceLoader();
    } else {
      cloudManager = SimCloudManager.createCluster(2, TimeSource.get("simTime:50"));
      // wait for defaults to be applied - due to accelerated time sometimes we may miss this
      cloudManager.getTimeSource().sleep(10000);
      AutoScalingConfig cfg = cloudManager.getDistribStateManager().getAutoScalingConfig();
      assertFalse("autoscaling config is empty", cfg.isEmpty());
      solrClient = ((SimCloudManager)cloudManager).simGetSolrClient();
      loader = ((SimCloudManager) cloudManager).getLoader();
    }
    timeSource = cloudManager.getTimeSource();
  }

  @After
  public void restoreDefaults() throws Exception {
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST,
        "{'set-trigger' : " + AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_DSL + "}");
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    AutoScalingConfig autoScalingConfig = cloudManager.getDistribStateManager().getAutoScalingConfig();
    if (autoScalingConfig.getTriggerListenerConfigs().containsKey("foo")) {
      String cmd = "{" +
          "'remove-listener' : {'name' : 'foo'}" +
          "}";
      response = solrClient.request(createAutoScalingRequest(SolrRequest.METHOD.POST, cmd));
      assertEquals(response.get("result").toString(), "success");
    }
    if (cloudManager instanceof SimCloudManager) {
      ((SimCloudManager) cloudManager).getSimClusterStateProvider().simDeleteAllCollections();
    } else {
      cluster.deleteAllCollections();
    }
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (cloudManager instanceof SimCloudManager) {
      cloudManager.close();
    }
    solrClient = null;
    cloudManager = null;
  }

  @Test
  public void testTrigger() throws Exception {
    String collectionName = "collection1";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 2).setMaxShardsPerNode(2);
    create.process(solrClient);
    CloudTestUtils.waitForState(cloudManager, "failed to create " + collectionName, collectionName,
        CloudTestUtils.clusterShape(2, 2));

    long waitForSeconds = 3 + random().nextInt(5);
    Map<String, Object> props = createTriggerProps(waitForSeconds);
    try (IndexSizeTrigger trigger = new IndexSizeTrigger("index_size_trigger")) {
      trigger.configure(loader, cloudManager, props);
      trigger.init();
      trigger.setProcessor(noFirstRunProcessor);
      trigger.run();

      for (int i = 0; i < 25; i++) {
        SolrInputDocument doc = new SolrInputDocument("id", "id-" + i);
        solrClient.add(collectionName, doc);
      }
      solrClient.commit();

      AtomicBoolean fired = new AtomicBoolean(false);
      AtomicReference<TriggerEvent> eventRef = new AtomicReference<>();
      trigger.setProcessor(event -> {
        if (fired.compareAndSet(false, true)) {
          eventRef.set(event);
          long currentTimeNanos = timeSource.getTimeNs();
          long eventTimeNanos = event.getEventTime();
          long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
          if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
            fail("processor was fired before the configured waitFor period: currentTimeNanos=" + currentTimeNanos + ", eventTimeNanos=" +  eventTimeNanos + ",waitForNanos=" + waitForNanos);
          }
        } else {
          fail("IndexSizeTrigger was fired more than once!");
        }
        return true;
      });
      trigger.run();
      TriggerEvent ev = eventRef.get();
      // waitFor delay - should not produce any event yet
      assertNull("waitFor not elapsed but produced an event", ev);
      timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));
      trigger.run();
      ev = eventRef.get();
      assertNotNull("should have fired an event", ev);
      List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>) ev.getProperty(TriggerEvent.REQUESTED_OPS);
      assertNotNull("should contain requestedOps", ops);
      assertEquals("number of ops", 2, ops.size());
      boolean shard1 = false;
      boolean shard2 = false;
      for (TriggerEvent.Op op : ops) {
        assertEquals(CollectionParams.CollectionAction.SPLITSHARD, op.getAction());
        Set<Pair<String, String>> hints = (Set<Pair<String, String>>)op.getHints().get(Suggester.Hint.COLL_SHARD);
        assertNotNull("hints", hints);
        assertEquals("hints", 1, hints.size());
        Pair<String, String> p = hints.iterator().next();
        assertEquals(collectionName, p.first());
        if (p.second().equals("shard1")) {
          shard1 = true;
        } else if (p.second().equals("shard2")) {
          shard2 = true;
        } else {
          fail("unexpected shard name " + p.second());
        }
      }
      assertTrue("shard1 should be split", shard1);
      assertTrue("shard2 should be split", shard2);
    }
  }

  private Map<String, Object> createTriggerProps(long waitForSeconds) {
    Map<String, Object> props = new HashMap<>();
    props.put("event", "indexSize");
    props.put("waitFor", waitForSeconds);
    props.put("enabled", true);
    props.put(IndexSizeTrigger.UNIT_PROP, IndexSizeTrigger.Unit.docs.toString());
    props.put(IndexSizeTrigger.ABOVE_PROP, 10);
    props.put(IndexSizeTrigger.BELOW_PROP, 2);
    List<Map<String, String>> actions = new ArrayList<>(3);
    Map<String, String> map = new HashMap<>(2);
    map.put("name", "compute_plan");
    map.put("class", "solr.ComputePlanAction");
    actions.add(map);
    map = new HashMap<>(2);
    map.put("name", "execute_plan");
    map.put("class", "solr.ExecutePlanAction");
    actions.add(map);
    props.put("actions", actions);
    return props;
  }
}
