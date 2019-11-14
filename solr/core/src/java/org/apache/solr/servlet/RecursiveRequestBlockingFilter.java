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

package org.apache.solr.servlet;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecursiveRequestBlockingFilter extends BaseSolrFilter {
  private static final String TRACKING_HEADER_NAME = "X-Recursion-Tracking-Id";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ConcurrentHashMap<String, AtomicInteger> trackingMap = new ConcurrentHashMap<>();

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    log.info("RecursiveRequestBlockingFilter.init called");
    log.info("         .---.");
    log.info("        /     \\");
    log.info("        \\.@-@./");
    log.info("        /`\\_/`\\");
    log.info("       //  _  \\\\");
    log.info("      | \\     )|_");
    log.info("     /`\\_`>  <_/ \\");
    log.info("rrbf \\__/'---'\\__/");
  }

  @Override
  public void destroy() {
  }

  // TODO: logging
  private static class TrackedHttpServletRequest extends HttpServletRequestWrapper {
    private final String trackId;

    private static String initializeTrackId(HttpServletRequest request, Supplier<String> trackIdSupplier) {
      String result = request.getHeader(TRACKING_HEADER_NAME);
      if (StringUtils.isNotBlank(result)) {
        log.debug("got existing recursion tracking id: {}", result);
        return null; // null designates that we already have a tracking id in the original request
      }
      result = trackIdSupplier.get();
      log.debug("initialized new recursion tracking id: {}", result);
      return trackIdSupplier.get();
    }

    public TrackedHttpServletRequest(HttpServletRequest request, Supplier<String> trackIdSupplier) {
      super(request);
      this.trackId = initializeTrackId(request, trackIdSupplier);
    }

    @Override
    public String getHeader(String name) {
      if (trackId != null && TRACKING_HEADER_NAME.equalsIgnoreCase(name)) {
        return trackId;
      }
      return super.getHeader(name);
    }

    @Override
    public Enumeration<String> getHeaderNames() {
      if (trackId == null) {
        return super.getHeaderNames();
      }
      List<String> result = Collections.list(super.getHeaderNames());
      result.add(TRACKING_HEADER_NAME);
      return Collections.enumeration(result);
    }

    @Override
    public Enumeration<String> getHeaders(String name) {
      if (trackId != null && TRACKING_HEADER_NAME.equalsIgnoreCase(name)) {
        return Collections.enumeration(Arrays.asList(trackId));
      }
      return super.getHeaders(name);
    }
  }

  private int getMaxDepth(HttpServletRequest request) {
    // TODO: configurable default + request param/header
    return 3;
  }

  private void checkMaxDepth(HttpServletRequest request, int currentDepth) throws ServletException {
    int maxDepth = getMaxDepth(request);
    if (currentDepth > maxDepth) {
      log.error("Maximum depth of {} recursive calls exceeded", maxDepth);
      throw new ServletException("Maximum depth of " + maxDepth + " recursive calls exceeded");
    }
  }

  private AtomicInteger getDepthCounter(String trackId) {
    AtomicInteger newCounter = new AtomicInteger(0);
    AtomicInteger result = trackingMap.putIfAbsent(trackId, newCounter);
    return result == null ? newCounter : result;
  }

  @Override
  public void doFilter(ServletRequest _request, ServletResponse _response, FilterChain chain)
      throws IOException, ServletException {

    TrackedHttpServletRequest request = new TrackedHttpServletRequest((HttpServletRequest) _request,
        () -> RandomStringUtils.random(16, true, true));
    String trackId = request.getHeader(TRACKING_HEADER_NAME);
    AtomicInteger depthCounter = getDepthCounter(trackId);

    try {
      checkMaxDepth(request, depthCounter.incrementAndGet());
      chain.doFilter(request, _response);
    } finally {
      if (depthCounter.decrementAndGet() == 0) {
        trackingMap.remove(trackId);
      }
    }
  }
}
