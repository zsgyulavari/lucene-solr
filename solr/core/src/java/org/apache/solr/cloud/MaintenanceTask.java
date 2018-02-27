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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public interface MaintenanceTask extends Runnable, Closeable {

  /**
   * Initialize this task. This method may be called multiple times when configuration changes.
   * @param properties configuration properties (may contain unrelated key / values)
   */
  void init(Map<String, Object> properties);

  /**
   * Schedule period in seconds. The task will be executed at most that frequently.
   */
  int getSchedulePeriod();

  /**
   * Name of this task (must be unique). Default value is the simple class name.
   * @return task name
   */
  default String getName() {
    return getClass().getSimpleName();
  }

  default void close() throws IOException {

  }
}
