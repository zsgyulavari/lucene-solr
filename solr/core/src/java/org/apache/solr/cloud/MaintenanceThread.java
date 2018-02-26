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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudManager;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.util.DefaultSolrThreadFactory;

/**
 * Responsible for running periodically some maintenance tasks.
 */
public class MaintenanceThread implements SolrCloseable {

  private class Task {
    MaintenanceTask maintenanceTask;
    ScheduledFuture<?> scheduledFuture;
  }

  private final SolrCloudManager cloudManager;
  private final ScheduledThreadPoolExecutor taskExecutor;
  private final Map<String, Task> tasks = new ConcurrentHashMap<>();

  private transient boolean isClosed = false;

  public MaintenanceThread(SolrCloudManager cloudManager) {
    this.cloudManager = cloudManager;
    this.taskExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(3, new DefaultSolrThreadFactory("MaintenanceTask"));
  }

  public void addTask(MaintenanceTask task, int period, TimeUnit timeUnit, boolean replace) throws AlreadyExistsException {
    if (tasks.containsKey(task.getName())) {
      if (replace) {
        Task oldTask = tasks.get(task.getName());
        IOUtils.closeQuietly(oldTask.maintenanceTask);
        if (oldTask.scheduledFuture != null) {
          oldTask.scheduledFuture.cancel(false);
        }
      } else {
        throw new AlreadyExistsException(task.getName());
      }
    }
    Task t = new Task();
    t.maintenanceTask = task;
    tasks.put(task.getName(), t);
    t.scheduledFuture = taskExecutor.scheduleWithFixedDelay(task, 0,
        cloudManager.getTimeSource().convertDelay(timeUnit, period, TimeUnit.MILLISECONDS),
        TimeUnit.MILLISECONDS);
  }

  public void removeTask(String name) {
    Task t = tasks.get(name);
    if (t == null) {
      // ignore
      return;
    }
    IOUtils.closeQuietly(t.maintenanceTask);
    if (t.scheduledFuture != null) {
      t.scheduledFuture.cancel(false);
    }
  }

  @Override
  public void close() throws IOException {
    isClosed = true;
    for (Task task : tasks.values()) {
      IOUtils.closeQuietly(task.maintenanceTask);
      if (task.scheduledFuture != null) {
        task.scheduledFuture.cancel(false);
      }
    }
    tasks.clear();
    taskExecutor.shutdown();
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }
}
