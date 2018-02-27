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
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for running periodically some maintenance tasks. Components can register maintenance tasks
 * using {@link #addTask(MaintenanceTask, boolean)} method. Task defines the period (in seconds) for how
 * often it should run.
 */
public class MaintenanceTasks implements Runnable, SolrCloseable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private class ScheduledTask {
    MaintenanceTask maintenanceTask;
    ScheduledFuture<?> scheduledFuture;
  }

  private final SolrCloudManager cloudManager;
  private final ScheduledThreadPoolExecutor taskExecutor;
  private final Map<String, ScheduledTask> tasks = new ConcurrentHashMap<>();
  private Object updated = new Object();

  private volatile Map<String, Object> clusterProperties = Collections.emptyMap();
  private volatile boolean isClosed = false;

  public MaintenanceTasks(SolrCloudManager cloudManager) {
    this.cloudManager = cloudManager;
    this.taskExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(3, new DefaultSolrThreadFactory("MaintenanceTask"));
  }

  @Override
  public void run() {
    refreshClusterProperties();
    while (!isClosed && !Thread.interrupted()) {
      try {
        synchronized (updated) {
          updated.wait();
        }
        // re-init and reschedule tasks
        for (ScheduledTask st : tasks.values()) {
          if (st.scheduledFuture != null) {
            st.scheduledFuture.cancel(false);
          }
          st.maintenanceTask.init(clusterProperties);
          st.scheduledFuture = taskExecutor.scheduleWithFixedDelay(st.maintenanceTask, 0,
              cloudManager.getTimeSource().convertDelay(TimeUnit.SECONDS, st.maintenanceTask.getSchedulePeriod(), TimeUnit.MILLISECONDS),
              TimeUnit.MILLISECONDS);
        }
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  private void refreshClusterProperties() {
    try {
      VersionedData data = cloudManager.getDistribStateManager().getData(ZkStateReader.CLUSTER_PROPS, ev -> {
        // session events are not change events, and do not remove the watcher
        if (ev.getType().equals(Watcher.Event.EventType.None)) {
          return;
        } else {
          refreshClusterProperties();
        }
      });
      Map<String, Object> newProperties = (Map<String, Object>)Utils.fromJSON(data.getData());
      clusterProperties = newProperties;
      synchronized (updated) {
        updated.notifyAll();
      }
    } catch (Exception e) {
      log.warn("Exception retrieving cluster properties", e);
    }
  }

  public void addTask(MaintenanceTask task, boolean replace) throws AlreadyExistsException {
    if (tasks.containsKey(task.getName())) {
      if (replace) {
        ScheduledTask oldScheduledTask = tasks.get(task.getName());
        IOUtils.closeQuietly(oldScheduledTask.maintenanceTask);
        if (oldScheduledTask.scheduledFuture != null) {
          oldScheduledTask.scheduledFuture.cancel(false);
        }
      } else {
        throw new AlreadyExistsException(task.getName());
      }
    }
    ScheduledTask st = new ScheduledTask();
    st.maintenanceTask = task;
    task.init(clusterProperties);
    tasks.put(task.getName(), st);
    st.scheduledFuture = taskExecutor.scheduleWithFixedDelay(task, 0,
        cloudManager.getTimeSource().convertDelay(TimeUnit.SECONDS, task.getSchedulePeriod(), TimeUnit.MILLISECONDS),
        TimeUnit.MILLISECONDS);
  }

  public void removeTask(String name) {
    ScheduledTask st = tasks.get(name);
    if (st == null) {
      // ignore
      return;
    }
    if (st.scheduledFuture != null) {
      st.scheduledFuture.cancel(false);
    }
    IOUtils.closeQuietly(st.maintenanceTask);
  }

  @Override
  public void close() throws IOException {
    isClosed = true;
    for (ScheduledTask scheduledTask : tasks.values()) {
      if (scheduledTask.scheduledFuture != null) {
        scheduledTask.scheduledFuture.cancel(false);
      }
      IOUtils.closeQuietly(scheduledTask.maintenanceTask);
    }
    tasks.clear();
    taskExecutor.shutdown();
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }
}
