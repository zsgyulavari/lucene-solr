package org.apache.solr.cloud.autoscaling;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.autoscaling.sim.SimCloudManager;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.LogLevel;
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
public class ScheduledMaintenanceTriggerTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static SolrCloudManager cloudManager;
  private static SolrClient solrClient;
  private static TimeSource timeSource;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    if (random().nextBoolean() || true) {
      cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
      solrClient = cluster.getSolrClient();
    } else {
      cloudManager = SimCloudManager.createCluster(1, TimeSource.get("simTime:50"));
      solrClient = ((SimCloudManager)cloudManager).simGetSolrClient();
    }
    timeSource = cloudManager.getTimeSource();
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
  public void testTriggerDefaults() throws Exception {
    AutoScalingConfig autoScalingConfig = cloudManager.getDistribStateManager().getAutoScalingConfig();
    log.info(autoScalingConfig.toString());
    AutoScalingConfig.TriggerConfig triggerConfig = autoScalingConfig.getTriggerConfigs().get(AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_NAME);
    assertNotNull(triggerConfig);
    assertEquals(1, triggerConfig.actions.size());
    assertTrue(triggerConfig.actions.get(0).actionClass.endsWith(InactiveShardCleanupAction.class.getSimpleName()));
    AutoScalingConfig.TriggerListenerConfig listenerConfig = autoScalingConfig.getTriggerListenerConfigs().get(AutoScaling.SCHEDULED_MAINTENANCE_LISTENER_NAME);
    assertNotNull(listenerConfig);
    assertEquals(AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_NAME, listenerConfig.trigger);
    assertTrue(listenerConfig.listenerClass.endsWith(SystemLogListener.class.getSimpleName()));
  }

  static Map<String, List<CapturedEvent>> listenerEvents = new ConcurrentHashMap<>();
  static CountDownLatch listenerCreated = new CountDownLatch(1);

  public static class CapturingTriggerListener extends TriggerListenerBase {
    @Override
    public void init(SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) {
      super.init(cloudManager, config);
      listenerCreated.countDown();
    }

    @Override
    public synchronized void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                                     ActionContext context, Throwable error, String message) {
      List<CapturedEvent> lst = listenerEvents.computeIfAbsent(config.name, s -> new ArrayList<>());
      lst.add(new CapturedEvent(timeSource.getTime(), context, config, stage, actionName, event, message));
    }
  }

  @Test
  public void testInactiveShardCleanup() throws Exception {
    String collection1 = getClass().getSimpleName() + "_collection1";
    String collection2 = getClass().getSimpleName() + "_collection2";
    CollectionAdminRequest.Create create1 = CollectionAdminRequest.createCollection(collection1,
        "conf", 1, 1);
    CollectionAdminRequest.Create create2 = CollectionAdminRequest.createCollection(collection2,
        "conf", 1, 1);

    create1.process(solrClient);
    CloudTestUtils.waitForState(cloudManager, "failed to create " + collection1, collection1,
        CloudTestUtils.clusterShape(1, 1));

    create2.process(solrClient);
    CloudTestUtils.waitForState(cloudManager, "failed to create " + collection2, collection2,
        CloudTestUtils.clusterShape(1, 1));

    CollectionAdminRequest.SplitShard split1 = CollectionAdminRequest.splitShard(collection1)
        .setShardName("shard1");
    split1.process(solrClient);
    CloudTestUtils.waitForState(cloudManager, "failed to split " + collection1, collection1,
        CloudTestUtils.clusterShape(3, 1));

    cloudManager.getTimeSource().sleep(10000);

    CollectionAdminRequest.SplitShard split2 = CollectionAdminRequest.splitShard(collection2)
        .setShardName("shard1");
    split2.process(solrClient);
    CloudTestUtils.waitForState(cloudManager, "failed to split " + collection2, collection2,
        CloudTestUtils.clusterShape(3, 1));

    cloudManager.getTimeSource().sleep(5000);

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'foo'," +
        "'trigger' : '" + AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_NAME + "'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED','FAILED']," +
        "'beforeAction' : 'inactive_shard_cleanup'," +
        "'afterAction' : 'inactive_shard_cleanup'," +
        "'class' : '" + CapturingTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : '" + AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_NAME + "'," +
        "'event' : 'scheduled'," +
        "'startTime' : 'NOW+5SECONDS'," +
        "'every' : '+2SECONDS'," +
        "'enabled' : true," +
        "'actions' : [{'name':'inactive_shard_cleanup', 'class' : 'solr.InactiveShardCleanupAction', 'ttl' : '10'}]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    boolean await = listenerCreated.await(10, TimeUnit.SECONDS);
    assertTrue("listener not created in time", await);
    cloudManager.getTimeSource().sleep(10000);
    // first cleanup should have occurred
    assertFalse("no events captured!", listenerEvents.isEmpty());
    List<CapturedEvent> events = new ArrayList<>(listenerEvents.get("foo"));
    listenerEvents.clear();

    assertEquals(8, events.size());
    CapturedEvent ev = events.get(2);
    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, ev.stage);
    Map<String, Object> map = (Map<String, Object>)ev.context.get("properties.inactive_shard_cleanup");
    assertNotNull(map);
    Map<String, List<String>> inactive = (Map<String, List<String>>)map.get("inactive");
    assertEquals(2, inactive.size());
    assertNotNull(inactive.get(collection1));
    assertNotNull(inactive.get(collection2));
    Map<String, List<String>> cleaned = (Map<String, List<String>>)map.get("cleaned");
    assertEquals(1, cleaned.size());
    assertNotNull(cleaned.get(collection1));
    assertNull(cleaned.get(collection2));

    ev = events.get(6);
    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, ev.stage);
    map = (Map<String, Object>)ev.context.get("properties.inactive_shard_cleanup");
    assertNotNull(map);
    inactive = (Map<String, List<String>>)map.get("inactive");
    assertEquals(1, inactive.size());
    assertNull(inactive.get(collection1));
    assertNotNull(inactive.get(collection2));
    cleaned = (Map<String, List<String>>)map.get("cleaned");
    assertEquals(0, cleaned.size());

    cloudManager.getTimeSource().sleep(10000);
    // the other cleanup should have occurred
    assertFalse("no events captured!", listenerEvents.isEmpty());
    events = new ArrayList<>(listenerEvents.get("foo"));
    assertEquals(8, events.size());
    ev = events.get(2);
    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, ev.stage);
    map = (Map<String, Object>)ev.context.get("properties.inactive_shard_cleanup");
    assertNotNull(map);
    inactive = (Map<String, List<String>>)map.get("inactive");
    assertEquals(1, inactive.size());
    assertNull(inactive.get(collection1));
    assertNotNull(inactive.get(collection2));
    cleaned = (Map<String, List<String>>)map.get("cleaned");
    assertEquals(1, cleaned.size());
    assertNotNull(cleaned.get(collection2));
    assertNull(cleaned.get(collection1));

    ev = events.get(6);
    map = (Map<String, Object>)ev.context.get("properties.inactive_shard_cleanup");
    assertNull(map);
  }
}
