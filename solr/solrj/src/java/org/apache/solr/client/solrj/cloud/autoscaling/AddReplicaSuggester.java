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

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;

class AddReplicaSuggester extends Suggester {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  SolrRequest init() {
    Set<String> collections = (Set<String>) hints.getOrDefault(Hint.COLL, Collections.emptySet());
    Set<Pair<String, String>> s = (Set<Pair<String, String>>) hints.getOrDefault(Hint.COLL_SHARD, Collections.emptySet());

    String withCollection = findWithCollection(collections, s);
    //if withCollection is present, try to allocate replicas to nodes which already have the withCollection replicas and
    // that do not have  any violations
    Collection originalTargetNodesCopy = setupWithCollectionTargetNodes(collections, s, withCollection);

    log.debug("Generating suggestions with hints: {}", hints);

    SolrRequest operation = tryEachNode(true);
    if (operation == null && withCollection != null)  {
      // it was not possible to place the replica in nodes which already have that withCollection.
      //try to place the replica in any node. This may add replica of withCollection to that node (if not present)
      // restore original target nodes and try again
      if (originalTargetNodesCopy != null)  {
        hints.put(Hint.TARGET_NODE, originalTargetNodesCopy);
      } else  {
        hints.remove(Hint.TARGET_NODE);
      }

      operation = tryEachNode(true);
      //we still can't get a valid node
      if (operation == null)  {
        for (Row row : getMatrix()) {
          row.forEachReplica(r -> {
            if(withCollection.equals(r.getCollection()))
              hint(Hint.TARGET_NODE, row.node);
          });
        }
    /*    getMatrix().stream()
            .filter(row -> row.collectionVsShardVsReplicas.containsKey(withCollection))
            .forEach(row -> hint(Hint.TARGET_NODE, row.node));*/

        // previously, we tried to allocate replicas without ignoring the non strict rules
        // (but they have the withCollection replicas)
        //now we are going to try by ignoring the no strict rules
        operation = tryEachNode(false);
        if (operation == null)  {
          // we still can't get a node where we can place a replica
          // restore original target nodes and try again
          if (originalTargetNodesCopy != null)  {
            hints.put(Hint.TARGET_NODE, originalTargetNodesCopy);
          } else  {
            hints.remove(Hint.TARGET_NODE);
          }
          operation = tryEachNode(false);
        }
      }
    } else if (operation == null) {
      operation = tryEachNode(false);
    }
    return operation;
  }

  SolrRequest tryEachNode(boolean strict) {
    Set<Pair<String, String>> shards = (Set<Pair<String, String>>) hints.getOrDefault(Hint.COLL_SHARD, Collections.emptySet());
    if (shards.isEmpty()) {
      throw new RuntimeException("add-replica requires 'collection' and 'shard'");
    }
    for (Pair<String, String> shard : shards) {
      Replica.Type type = Replica.Type.get((String) hints.get(Hint.REPLICATYPE));
      // iterate through nodes and identify the least loaded
      List<Violation> leastSeriousViolation = null;
      Row bestNode = null;
      for (int i = getMatrix().size() - 1; i >= 0; i--) {
        Row row = getMatrix().get(i);
        if (!isNodeSuitableForReplicaAddition(row)) continue;
        Row tmpRow = row.addReplica(shard.first(), shard.second(), type);
        List<Violation> errs = testChangedMatrix(strict, tmpRow.session);

        if (!containsNewErrors(errs)) {
          if (isLessSerious(errs, leastSeriousViolation)) {
            leastSeriousViolation = errs;
            bestNode = tmpRow;
          }
        }
      }

      if (bestNode != null) {// there are no rule violations
        this.session = bestNode.session;
        return CollectionAdminRequest
            .addReplicaToShard(shard.first(), shard.second())
            .setType(type)
            .setNode(bestNode.node);
      }
    }

    return null;
  }

  @Override
  public CollectionParams.CollectionAction getAction() {
    return ADDREPLICA;
  }
}
