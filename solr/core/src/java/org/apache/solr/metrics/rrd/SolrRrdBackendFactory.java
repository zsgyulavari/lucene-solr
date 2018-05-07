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
package org.apache.solr.metrics.rrd;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.rrd4j.core.RrdBackend;
import org.rrd4j.core.RrdBackendFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SolrRrdBackendFactory extends RrdBackendFactory implements SolrCloseable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int DEFAULT_SYNC_PERIOD = 60;
  public static final int DEFAULT_MAX_DBS = 500;

  public static final String ID_PREFIX = "rrd_";
  public static final String DOC_TYPE = "metrics_rrd";

  public static final String DATA_FIELD = "data_bin";

  private final SolrClient solrClient;
  private final String collection;
  private ScheduledThreadPoolExecutor syncService;
  private int syncPeriod = DEFAULT_SYNC_PERIOD;
  private volatile boolean closed = false;

  private final Map<String, SolrRrdBackend> backends = new ConcurrentHashMap<>();

  public SolrRrdBackendFactory(SolrClient solrClient, String collection) {
    this.solrClient = solrClient;
    this.collection = collection;
    syncService = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(2,
        new DefaultSolrThreadFactory("SolrRrdBackendFactory"));
    syncService.setRemoveOnCancelPolicy(true);
    syncService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    syncService.scheduleWithFixedDelay(() -> maybeSyncBackends(), syncPeriod, syncPeriod, TimeUnit.SECONDS);
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException("Factory already closed");
    }
  }

  @Override
  protected synchronized RrdBackend open(String path, boolean readOnly) throws IOException {
    ensureOpen();
    SolrRrdBackend backend = backends.computeIfAbsent(path, p -> new SolrRrdBackend(p, readOnly, this));
    if (backend.isReadOnly()) {
      if (readOnly) {
        return backend;
      } else {
        // replace it with a writable one
        backend = new SolrRrdBackend(path, readOnly, this);
        backends.put(path, backend);
        return backend;
      }
    } else {
      if (readOnly) {
        // return a throwaway read-only copy
        return new SolrRrdBackend(backend);
      } else {
        return backend;
      }
    }
  }

  byte[] getData(String path) throws IOException {
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(CommonParams.Q, "{!term f=id}" + ID_PREFIX + path);
      params.add(CommonParams.FQ, CommonParams.TYPE + ":" + DOC_TYPE);
      QueryResponse rsp = solrClient.query(collection, params);
      SolrDocumentList docs = rsp.getResults();
      if (docs == null || docs.isEmpty()) {
        return null;
      }
      if (docs.size() > 1) {
        throw new SolrServerException("Expected at most 1 doc with id '" + ID_PREFIX + path + "' but got " + docs);
      }
      SolrDocument doc = docs.get(0);
      Object o = doc.getFieldValue(DATA_FIELD);
      if (o == null) {
        return null;
      }
      if (o instanceof byte[]) {
        return (byte[])o;
      } else {
        throw new SolrServerException("Unexpected value of '" + DATA_FIELD + "' field: " + o.getClass().getName() + ": " + o);
      }
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  void unregisterBackend(String path) {
    backends.remove(path);
  }

  public List<String> list() throws IOException {
    ArrayList<String> names = new ArrayList<>();
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(CommonParams.Q, "*:*");
      params.add(CommonParams.FQ, CommonParams.TYPE + ":" + DOC_TYPE);
      params.add(CommonParams.FL, "id");
      params.add(CommonParams.SORT, "id asc");
      params.add(CommonParams.ROWS, String.valueOf(DEFAULT_MAX_DBS));
      QueryResponse rsp = solrClient.query(collection, params);
      SolrDocumentList docs = rsp.getResults();
      if (docs != null) {
        docs.forEach(d -> names.add(((String)d.getFieldValue("id")).substring(ID_PREFIX.length())));
      }
    } catch (SolrServerException e) {
      log.warn("Error retrieving RRD list", e);
    }
    return names;
  }

  public void remove(String path) throws IOException {
    SolrRrdBackend backend = backends.get(path);
    if (backend != null) {
      IOUtils.closeQuietly(backend);
    }
    // remove Solr doc
    try {
      solrClient.deleteByQuery(collection, "{!term f=id}" + ID_PREFIX + path);
    } catch (SolrServerException e) {
      log.warn("Error deleting RRD for path " + path, e);
    }
  }

  public synchronized void maybeSyncBackends() {
    if (closed) {
      return;
    }
    Map<String, byte[]> syncData = new HashMap<>();
    backends.forEach((path, backend) -> {
      byte[] data = backend.getSyncData();
      if (data != null) {
        syncData.put(backend.getPath(), data);
      }
    });
    if (syncData.isEmpty()) {
      return;
    }
    // write updates
    try {
      syncData.forEach((path, data) -> {
        SolrInputDocument doc = new SolrInputDocument();
        doc.setField("id", ID_PREFIX + path);
        doc.addField(CommonParams.TYPE, DOC_TYPE);
        doc.addField(DATA_FIELD, data);
        try {
          solrClient.add(collection, doc);
        } catch (SolrServerException | IOException e) {
          log.warn("Error updating RRD data for " + path, e);
        }
      });
      try {
        solrClient.commit(collection);
      } catch (SolrServerException e) {
        log.warn("Error committing RRD data updates", e);
      }
    } catch (IOException e) {
      log.warn("Error sending RRD data updates", e);
    }
  }

  @Override
  protected boolean exists(String path) throws IOException {
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(CommonParams.Q, "{!term f=id}" + ID_PREFIX + path);
      params.add(CommonParams.FQ, CommonParams.TYPE + ":" + DOC_TYPE);
      params.add(CommonParams.FL, "id");
      QueryResponse rsp = solrClient.query(collection, params);
      SolrDocumentList docs = rsp.getResults();
      if (docs == null || docs.isEmpty()) {
        return false;
      }
      if (docs.size() > 1) {
        throw new SolrServerException("Expected at most 1 doc with id '" + ID_PREFIX + path + "' but got " + docs);
      }
      return true;
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected boolean shouldValidateHeader(String path) throws IOException {
    return false;
  }

  @Override
  public String getName() {
    return "SOLR";
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    backends.forEach((p, b) -> IOUtils.closeQuietly(b));
    backends.clear();
    syncService.shutdown();
    syncService = null;
  }
}
