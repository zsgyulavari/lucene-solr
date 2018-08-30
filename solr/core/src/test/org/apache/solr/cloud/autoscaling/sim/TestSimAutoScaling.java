package org.apache.solr.cloud.autoscaling.sim;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.autoscaling.ExecutePlanAction;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;

/**
 *
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.cloud.autoscaling.NodeLostTrigger=INFO;org.apache.client.solrj.cloud.autoscaling=DEBUG")
public class TestSimAutoScaling extends SimSolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int SPEED = 50;

  private static TimeSource timeSource;
  private static SolrClient solrClient;

  @BeforeClass
  public static void setupCluster() throws Exception {
    // 20 mln docs / node
    configureCluster(50, TimeSource.get("simTime:" + SPEED));
    timeSource = cluster.getTimeSource();
    solrClient = cluster.simGetSolrClient();
  }

  @Test
  public void testScaleUp() throws Exception {
    String collectionName = "testScaleUp_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 2).setMaxShardsPerNode(10);
    create.process(solrClient);
    CloudTestUtils.waitForState(cluster, "failed to create " + collectionName, collectionName,
        CloudTestUtils.clusterShape(2, 2, false, true));

    long waitForSeconds = 3 + random().nextInt(5);
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'scaleUpTrigger'," +
        "'event' : 'indexSize'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'aboveDocs' : 5000000," +
        "'enabled' : true," +
        "'actions' : [{'name' : 'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name' : 'execute_plan', 'class' : '" + ExecutePlanAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    long batchSize = 250000;
    for (long i = 0; i < 10000; i++) {
      log.info("#### Total docs so far: " + (i * batchSize));
      addDocs(collectionName, i * batchSize, batchSize);
      timeSource.sleep(waitForSeconds);
    }
  }

  private void addDocs(String collection, long start, long count) throws Exception {
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParam("collection", collection);
    ureq.setDocIterator(new FakeDocIterator(start, count));
    solrClient.request(ureq);
  }

  // lightweight generator of fake documents
  private static class FakeDocIterator implements Iterator<SolrInputDocument> {
    final SolrInputDocument doc = new SolrInputDocument();
    final SolrInputField idField = new SolrInputField("id");

    final long start, count;
    final StringBuilder sb = new StringBuilder("id-");

    long current, max;

    FakeDocIterator(long start, long count) {
      this.start = start;
      this.count = count;
      current = start;
      max = start + count;
      doc.put("id", idField);
    }

    @Override
    public boolean hasNext() {
      return current < max;
    }

    @Override
    public SolrInputDocument next() {
      sb.setLength(3);
      idField.setValue(sb.append(Long.toString(current)).toString());
      current++;
      return doc;
    }
  }

}
