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
package org.apache.solr.handler.admin;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.rrd.SolrRrdBackendFactory;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.rrd4j.ConsolFun;
import org.rrd4j.DsType;
import org.rrd4j.core.RrdBackendFactory;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;
import org.rrd4j.core.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MetricsHistoryHandler extends RequestHandlerBase implements PermissionNameProvider, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final Set<String> DEFAULT_CORE_COUNTERS = new HashSet<String>() {{
    add("QUERY./select.requests");
    add("INDEX.sizeInBytes");
    add("UPDATE./update.requests");
  }};
  public static final Set<String> DEFAULT_CORE_GAUGES = new HashSet<String>() {{
    add("INDEX.sizeInBytes");
  }};
  public static final Set<String> DEFAULT_NODE_GAUGES = new HashSet<String>() {{
    add("CONTAINER.fs.coreRoot.usableSpace");
  }};
  public static final Set<String> DEFAULT_JVM_GAUGES = new HashSet<String>() {{
    add("memory.heap.used");
    add("os.processCpuLoad");
    add("os.systemLoadAverage");
  }};

  public static final int DEFAULT_COLLECT_PERIOD = 60;
  public static final String URI_PREFIX = "solr://";

  private final SolrRrdBackendFactory factory;
  private final SolrClient solrClient;
  private final MetricsHandler metricsHandler;
  private final SolrMetricManager metricManager;
  private final SolrCloudManager cloudManager;
  private final ScheduledThreadPoolExecutor collectService;
  private final TimeSource timeSource;
  private final int collectPeriod;
  private final Map<String, Set<String>> counters = new HashMap<>();
  private final Map<String, Set<String>> gauges = new HashMap<>();

  private boolean logMissingCollection = true;

  public MetricsHistoryHandler(SolrMetricManager metricManager, MetricsHandler metricsHandler,
                               SolrClient solrClient, SolrCloudManager cloudManager) {
    factory = new SolrRrdBackendFactory(solrClient, CollectionAdminParams.SYSTEM_COLL,
        SolrRrdBackendFactory.DEFAULT_SYNC_PERIOD, cloudManager.getTimeSource());
    RrdBackendFactory.registerAndSetAsDefaultFactory(factory);
    this.solrClient = solrClient;
    this.metricsHandler = metricsHandler;
    this.metricManager = metricManager;
    this.cloudManager = cloudManager;
    collectPeriod = DEFAULT_COLLECT_PERIOD;
    this.timeSource = cloudManager.getTimeSource();

    counters.put(Group.core.toString(), DEFAULT_CORE_COUNTERS);
    counters.put(Group.node.toString(), Collections.emptySet());
    counters.put(Group.jvm.toString(), Collections.emptySet());
    gauges.put(Group.core.toString(), DEFAULT_CORE_GAUGES);
    gauges.put(Group.node.toString(), DEFAULT_NODE_GAUGES);
    gauges.put(Group.jvm.toString(), DEFAULT_JVM_GAUGES);

    collectService = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(2,
        new DefaultSolrThreadFactory("SolrRrdBackendFactory"));
    collectService.setRemoveOnCancelPolicy(true);
    collectService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    collectService.scheduleWithFixedDelay(() -> collectMetrics(), collectPeriod, collectPeriod, TimeUnit.SECONDS);
  }

  public SolrClient getSolrClient() {
    return solrClient;
  }

  private void collectMetrics() {
    // check that .system exists
    try {
      ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
      if (clusterState.getCollectionOrNull(CollectionAdminParams.SYSTEM_COLL) == null) {
        if (logMissingCollection) {
          log.warn("Missing " + CollectionAdminParams.SYSTEM_COLL + ", skipping metrics collection");
          logMissingCollection = false;
        }
        return;
      }
    } catch (Exception e) {
      log.warn("Error getting cluster state, skipping metrics collection", e);
      return;
    }
    logMissingCollection = true;
    // get metrics
    for (Group group : Arrays.asList(Group.core, Group.node, Group.jvm)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(MetricsHandler.GROUP_PARAM, group.toString());
      params.add(MetricsHandler.COMPACT_PARAM, "true");
      counters.get(group.toString()).forEach(c -> params.add(MetricsHandler.PREFIX_PARAM, c));
      gauges.get(group.toString()).forEach(c -> params.add(MetricsHandler.PREFIX_PARAM, c));
      AtomicReference<Object> result = new AtomicReference<>();
      try {
        metricsHandler.handleRequest(params, (k, v) -> {
          if (k.equals("metrics")) {
            result.set(v);
          }
        });
        NamedList nl = (NamedList)result.get();
        if (nl != null) {
          for (Iterator<Map.Entry<String, Object>> it = nl.iterator(); it.hasNext(); ) {
            Map.Entry<String, Object> entry = it.next();
            final String registry = entry.getKey();
            RrdDb db = metricManager.getOrCreateMetricHistory(registry, () -> {
              RrdDef def = createDef(registry, group);
              try {
                RrdDb newDb = new RrdDb(def);
                return newDb;
              } catch (IOException e) {
                return null;
              }
            });
            if (db == null) {
              continue;
            }
            // set the timestamp
            Sample s = db.createSample(TimeUnit.MILLISECONDS.convert(timeSource.getEpochTimeNs(), TimeUnit.NANOSECONDS));
            NamedList<Object> values = (NamedList<Object>)entry.getValue();
            AtomicBoolean dirty = new AtomicBoolean(false);
            counters.get(group.toString()).forEach(c -> {
              Number val = (Number)values.get(c);
              if (val != null) {
                dirty.set(true);
                s.setValue(c, val.doubleValue());
              }
            });
            gauges.get(group.toString()).forEach(c -> {
              Number val = (Number)values.get(c);
              if (val != null) {
                dirty.set(true);
                s.setValue(c, val.doubleValue());
              }
            });
            if (dirty.get()) {
              s.update();
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  RrdDef createDef(String registry, Group group) {
    registry = SolrMetricManager.overridableRegistryName(registry);

    // base sampling period is collectPeriod - samples more frequent than
    // that will be dropped, samples less frequent will be interpolated
    RrdDef def = new RrdDef(URI_PREFIX + registry, collectPeriod);
    def.setStartTime(TimeUnit.MILLISECONDS.convert(timeSource.getEpochTimeNs(), TimeUnit.NANOSECONDS));

    // add datasources

    // use NaN when more than 1 sample is missing
    counters.get(group.toString()).forEach(c ->
        def.addDatasource(c, DsType.COUNTER, collectPeriod * 2, Double.NaN, Double.NaN));
    gauges.get(group.toString()).forEach(g ->
        def.addDatasource(g, DsType.GAUGE, collectPeriod * 2, Double.NaN, Double.NaN));

    // add archives

    // use AVERAGE consolidation,
    // use NaN when >50% samples are missing
    def.addArchive(ConsolFun.AVERAGE, 0.5, 1, 120); // 2 hours
    def.addArchive(ConsolFun.AVERAGE, 0.5, 10, 288); // 48 hours
    def.addArchive(ConsolFun.AVERAGE, 0.5, 60, 336); // 2 weeks
    def.addArchive(ConsolFun.AVERAGE, 0.5, 240, 180); // 2 months
    return def;
  }

  @Override
  public void close() {
    if (collectService != null) {
      collectService.shutdown();
    }
    if (factory != null) {
      factory.close();
    }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {

  }

  @Override
  public String getDescription() {
    return "A handler for metrics history";
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.METRICS_HISTORY_READ_PERM;
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Collection<Api> getApis() {
    return ApiBag.wrapRequestHandlers(this, "metrics.history");
  }

}
