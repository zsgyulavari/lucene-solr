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
import org.apache.solr.common.cloud.ClusterState;
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
    if (random().nextBoolean() && false) {
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
      CapturedEvent ev = new CapturedEvent(timeSource.getTime(), context, config, stage, actionName, event, message);
      log.info("=======> " + ev);
      lst.add(ev);
    }
  }

  static CountDownLatch triggerFired = new CountDownLatch(1);

  public static class TestTriggerAction extends TriggerActionBase {

    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      if (context.getProperties().containsKey("inactive_shard_cleanup")) {
        triggerFired.countDown();
      }
    }
  }

  @Test
  public void testInactiveShardCleanup() throws Exception {
    String collection1 = getClass().getSimpleName() + "_collection1";
    CollectionAdminRequest.Create create1 = CollectionAdminRequest.createCollection(collection1,
        "conf", 1, 1);

    create1.process(solrClient);
    CloudTestUtils.waitForState(cloudManager, "failed to create " + collection1, collection1,
        CloudTestUtils.clusterShape(1, 1));

    CollectionAdminRequest.SplitShard split1 = CollectionAdminRequest.splitShard(collection1)
        .setShardName("shard1");
    split1.process(solrClient);
    CloudTestUtils.waitForState(cloudManager, "failed to split " + collection1, collection1,
        CloudTestUtils.clusterShape(3, 1));

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
        "'startTime' : 'NOW+3SECONDS'," +
        "'every' : '+2SECONDS'," +
        "'enabled' : true," +
        "'actions' : [{'name' : 'inactive_shard_cleanup', 'class' : 'solr.InactiveShardCleanupAction', 'ttl' : '10'}," +
        "{'name' : 'test', 'class' : '" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    boolean await = listenerCreated.await(10, TimeUnit.SECONDS);
    assertTrue("listener not created in time", await);
    await = triggerFired.await(20, TimeUnit.SECONDS);
    assertTrue("cleanup action didn't run", await);

    // first cleanup should have occurred
    assertFalse("no events captured!", listenerEvents.isEmpty());
    List<CapturedEvent> events = new ArrayList<>(listenerEvents.get("foo"));
    listenerEvents.clear();

    assertFalse(events.isEmpty());
    int inactiveEvents = 0;
    CapturedEvent ce = null;
    for (CapturedEvent e : events) {
      if (e.stage != TriggerEventProcessorStage.AFTER_ACTION) {
        continue;
      }
      if (e.context.containsKey("properties.inactive_shard_cleanup")) {
        ce = e;
        break;
      } else {
        inactiveEvents++;
      }
    }
    assertTrue("should be at least one inactive event", inactiveEvents > 0);
    assertNotNull("missing cleanup event", ce);
    Map<String, Object> map = (Map<String, Object>)ce.context.get("properties.inactive_shard_cleanup");
    assertNotNull(map);

    Map<String, List<String>> inactive = (Map<String, List<String>>)map.get("inactive");
    assertEquals(1, inactive.size());
    assertNotNull(inactive.get(collection1));
    Map<String, List<String>> cleaned = (Map<String, List<String>>)map.get("cleaned");
    assertEquals(1, cleaned.size());
    assertNotNull(cleaned.get(collection1));

    ClusterState state = cloudManager.getClusterStateProvider().getClusterState();

    CloudTestUtils.clusterShape(2, 1).matches(state.getLiveNodes(), state.getCollection(collection1));
  }
}
