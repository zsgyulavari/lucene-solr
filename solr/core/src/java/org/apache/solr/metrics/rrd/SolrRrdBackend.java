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

import java.io.Closeable;
import java.io.IOException;

import org.rrd4j.core.RrdByteArrayBackend;

/**
 *
 */
public class SolrRrdBackend extends RrdByteArrayBackend implements Closeable {

  private final SolrRrdBackendFactory factory;
  private final boolean readOnly;
  private volatile boolean dirty = false;
  private volatile boolean closed = false;

  public SolrRrdBackend(String path, boolean readOnly, SolrRrdBackendFactory factory) {
    super(path);
    this.readOnly = readOnly;
    this.factory = factory;
  }

  /**
   * Open an unregistered read-only clone of the backend.
   * @param other other backend
   */
  public SolrRrdBackend(SolrRrdBackend other) {
    super(other.getPath());
    readOnly = true;
    factory = null;
    byte[] otherBuffer = other.buffer;
    buffer = new byte[otherBuffer.length];
    System.arraycopy(otherBuffer, 0, buffer, 0, otherBuffer.length);
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  @Override
  protected synchronized void write(long offset, byte[] bytes) throws IOException {
    if (readOnly || closed) {
      return;
    }
    super.write(offset, bytes);
    dirty = true;
  }

  public byte[] maybeSync() {
    if (readOnly || closed) {
      return null;
    }
    if (!dirty) {
      return null;
    }
    byte[] bufferCopy = new byte[buffer.length];
    System.arraycopy(buffer, 0, bufferCopy, 0, buffer.length);
    return bufferCopy;
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;
    if (factory != null) {
      // unregister myself from the factory
      factory.unregisterBackend(getPath());
    }
    // close
  }
}
