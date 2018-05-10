package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.autoscaling.sim.SimCloudManager;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@LuceneTestCase.Slow
@LogLevel("org.apache.solr.handler.admin=DEBUG")
public class MetricsHistoryIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static SolrCloudManager cloudManager;
  private static SolrClient solrClient;
  private static TimeSource timeSource;
  private static SolrResourceLoader loader;

  private static int SPEED;

  @BeforeClass
  public static void setupCluster() throws Exception {
    boolean simulated = random().nextBoolean() && false;
    if (simulated) {
      SPEED = 50;
      cloudManager = SimCloudManager.createCluster(2, TimeSource.get("simTime:" + SPEED));
      solrClient = ((SimCloudManager)cloudManager).simGetSolrClient();
      loader = ((SimCloudManager) cloudManager).getLoader();
    }
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    if (!simulated) {
      cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
      solrClient = cluster.getSolrClient();
      loader = cluster.getJettySolrRunner(0).getCoreContainer().getResourceLoader();
      SPEED = 1;
    }
    timeSource = cloudManager.getTimeSource();
    // create .system
    CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL, null, 1, 2)
        .process(solrClient);
    CloudTestUtils.waitForState(cloudManager, CollectionAdminParams.SYSTEM_COLL,
        30, TimeUnit.SECONDS, CloudTestUtils.clusterShape(1, 2));
    // sleep a little to allow handler to collect some metrics
    timeSource.sleep(90000);
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
  public void testList() throws Exception {
    NamedList<Object> rsp = solrClient.request(createHistoryRequest(params(CommonParams.ACTION, "list")));
    assertNotNull(rsp);
  }

  public static SolrRequest createHistoryRequest(SolrParams params) {
    return new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/metrics/history", params);
  }

}
