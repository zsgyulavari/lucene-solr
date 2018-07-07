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
package org.apache.solr.update;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.HardlinkCopyDirectoryWrapper;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.HashBasedRouter;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.IndexFetcher;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.BitsFilteredPostingsEnum;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrIndexSplitter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  SolrIndexSearcher searcher;
  SchemaField field;
  List<DocRouter.Range> ranges;
  DocRouter.Range[] rangesArr; // same as ranges list, but an array for extra speed in inner loops
  List<String> paths;
  List<SolrCore> cores;
  DocRouter router;
  HashBasedRouter hashRouter;
  int numPieces;
  int currPartition = 0;
  String routeFieldName;
  String splitKey;
  boolean offline;

  public SolrIndexSplitter(SplitIndexCommand cmd) {
    searcher = cmd.getReq().getSearcher();
    ranges = cmd.ranges;
    paths = cmd.paths;
    cores = cmd.cores;
    router = cmd.router;
    hashRouter = router instanceof HashBasedRouter ? (HashBasedRouter)router : null;

    if (ranges == null) {
      numPieces =  paths != null ? paths.size() : cores.size();
    } else  {
      numPieces = ranges.size();
      rangesArr = ranges.toArray(new DocRouter.Range[ranges.size()]);
    }
    routeFieldName = cmd.routeFieldName;
    if (routeFieldName == null) {
      field = searcher.getSchema().getUniqueKeyField();
    } else  {
      field = searcher.getSchema().getField(routeFieldName);
    }
    if (cmd.splitKey != null) {
      splitKey = getRouteKey(cmd.splitKey);
    }
    if (cores == null) {
      this.offline = false;
    } else {
      this.offline = cmd.offline;
    }
  }

  public void split() throws IOException {

    List<LeafReaderContext> leaves = searcher.getRawReader().leaves();
    Directory parentDirectory = searcher.getRawReader().directory();
    List<FixedBitSet[]> segmentDocSets = new ArrayList<>(leaves.size());
    SolrIndexConfig parentConfig = searcher.getCore().getSolrConfig().indexConfig;

    log.info("SolrIndexSplitter: partitions=" + numPieces + " segments="+leaves.size());

    if (offline) {
      // close the searcher if using offline method
      // caller should have already locked the SolrCoreState.indexWriterLock at this point
      // thus preventing the creation of new IndexWriter
      searcher.getCore().closeSearcher();
      searcher = null;
    } else {
      for (LeafReaderContext readerContext : leaves) {
        assert readerContext.ordInParent == segmentDocSets.size();  // make sure we're going in order
        FixedBitSet[] docSets = split(readerContext);
        segmentDocSets.add(docSets);
      }
    }

    // would it be more efficient to write segment-at-a-time to each new index?
    // - need to worry about number of open descriptors
    // - need to worry about if IW.addIndexes does a sync or not...
    // - would be more efficient on the read side, but prob less efficient merging

    for (int partitionNumber=0; partitionNumber<numPieces; partitionNumber++) {
      String partitionName = "SolrIndexSplitter:partition=" + partitionNumber + ",partitionCount=" + numPieces + (ranges != null ? ",range=" + ranges.get(partitionNumber) : "");
      log.info(partitionName);

      boolean success = false;

      RefCounted<IndexWriter> iwRef = null;
      IndexWriter iw;
      if (cores != null && !offline) {
        SolrCore subCore = cores.get(partitionNumber);
        iwRef = subCore.getUpdateHandler().getSolrCoreState().getIndexWriter(subCore);
        iw = iwRef.get();
      } else {
        if (offline) {
          SolrCore subCore = cores.get(partitionNumber);
          String path = subCore.getDataDir() + "index.split";
          // copy by hard-linking
          Directory splitDir = subCore.getDirectoryFactory().get(path, DirectoryFactory.DirContext.DEFAULT, subCore.getSolrConfig().indexConfig.lockType);
          Directory hardLinkedDir = new HardlinkCopyDirectoryWrapper(splitDir);
          for (String file : parentDirectory.listAll()) {
            // there should be no write.lock
            if (file.equals(IndexWriter.WRITE_LOCK_NAME)) {
              throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "Splitting in 'offline' mode but parent write.lock exists!");
            }
            hardLinkedDir.copyFrom(parentDirectory, file, file, IOContext.DEFAULT);
          }
          IndexWriterConfig iwConfig = parentConfig.toIndexWriterConfig(subCore);
          iw = new SolrIndexWriter(partitionName, splitDir, iwConfig);
        } else {
          SolrCore core = searcher.getCore();
          String path = paths.get(partitionNumber);
          iw = SolrIndexWriter.create(core, partitionName, path,
              core.getDirectoryFactory(), true, core.getLatestSchema(),
              core.getSolrConfig().indexConfig, core.getDeletionPolicy(), core.getCodec());
        }
      }

      try {
        if (offline) {
          iw.deleteDocuments(new ShardSplitingQuery(partitionNumber, field, rangesArr, router, splitKey));
        } else {
          // This removes deletions but optimize might still be needed because sub-shards will have the same number of segments as the parent shard.
          for (int segmentNumber = 0; segmentNumber<leaves.size(); segmentNumber++) {
            log.info("SolrIndexSplitter: partition #" + partitionNumber + " partitionCount=" + numPieces + (ranges != null ? " range=" + ranges.get(partitionNumber) : "") + " segment #"+segmentNumber + " segmentCount=" + leaves.size());
            CodecReader subReader = SlowCodecReaderWrapper.wrap(leaves.get(segmentNumber).reader());
            iw.addIndexes(new LiveDocsReader(subReader, segmentDocSets.get(leaves.get(segmentNumber).ord)[partitionNumber]));
          }
        }
        // we commit explicitly instead of sending a CommitUpdateCommand through the processor chain
        // because the sub-shard cores will just ignore such a commit because the update log is not
        // in active state at this time.
        //TODO no commitUpdateCommand
        SolrIndexWriter.setCommitData(iw, -1);
        iw.commit();
        success = true;
      } finally {
        if (iwRef != null) {
          iwRef.decref();
        } else {
          if (success) {
            iw.close();
          } else {
            IOUtils.closeWhileHandlingException(iw);
          }
          if (offline) {
            SolrCore subCore = cores.get(partitionNumber);
            subCore.getDirectoryFactory().release(iw.getDirectory());
          }
        }
      }
    }
    // all sub-indexes created ok
    // when using hard-linking switch directories & refresh cores
    if (offline && cores != null) {
      boolean switchOk = true;
      for (int partitionNumber = 0; partitionNumber < numPieces; partitionNumber++) {
        SolrCore subCore = cores.get(partitionNumber);
        String indexDirPath = subCore.getIndexDir();

        log.debug("Switching directories");
        String hardLinkPath = subCore.getDataDir() + "index.split";
        subCore.modifyIndexProps("index.split");
        try {
          subCore.getUpdateHandler().newIndexWriter(false);
          openNewSearcher(subCore);
        } catch (Exception e) {
          log.error("Failed to switch sub-core " + indexDirPath + " to " + hardLinkPath + ", split will fail.", e);
          switchOk = false;
          break;
        }
      }
      if (!switchOk) {
        // rollback the switch
        for (int partitionNumber = 0; partitionNumber < numPieces; partitionNumber++) {
          SolrCore subCore = cores.get(partitionNumber);
          Directory dir = null;
          try {
            dir = subCore.getDirectoryFactory().get(subCore.getDataDir(), DirectoryFactory.DirContext.META_DATA,
                subCore.getSolrConfig().indexConfig.lockType);
            dir.deleteFile(IndexFetcher.INDEX_PROPERTIES);
          } finally {
            if (dir != null) {
              subCore.getDirectoryFactory().release(dir);
            }
          }
          // switch back if necessary and remove the hardlinked dir
          String hardLinkPath = subCore.getDataDir() + "index.split";
          try {
            dir = subCore.getDirectoryFactory().get(hardLinkPath, DirectoryFactory.DirContext.DEFAULT,
                subCore.getSolrConfig().indexConfig.lockType);
            subCore.getDirectoryFactory().doneWithDirectory(dir);
            subCore.getDirectoryFactory().remove(dir);
          } finally {
            if (dir != null) {
              subCore.getDirectoryFactory().release(dir);
            }
          }
          subCore.getUpdateHandler().newIndexWriter(false);
          try {
            openNewSearcher(subCore);
          } catch (Exception e) {
            log.warn("Error rolling back failed split of " + hardLinkPath, e);
          }
        }
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "There were errors during index split");
      } else {
        // complete the switch - remove original index
        for (int partitionNumber = 0; partitionNumber < numPieces; partitionNumber++) {
          SolrCore subCore = cores.get(partitionNumber);
          String oldIndexPath = subCore.getDataDir() + "index";
          Directory indexDir = null;
          try {
            indexDir = subCore.getDirectoryFactory().get(oldIndexPath,
                DirectoryFactory.DirContext.DEFAULT, subCore.getSolrConfig().indexConfig.lockType);
            subCore.getDirectoryFactory().doneWithDirectory(indexDir);
            subCore.getDirectoryFactory().remove(indexDir);
          } finally {
            if (indexDir != null) {
              subCore.getDirectoryFactory().release(indexDir);
            }
          }
        }
      }
    }
  }

  private void openNewSearcher(SolrCore core) throws Exception {
    Future[] waitSearcher = new Future[1];
    core.getSearcher(true, false, waitSearcher, true);
    if (waitSearcher[0] != null) {
      waitSearcher[0].get();
    }
  }

  private static class ShardSplitingQuery extends Query {
    final private int partition;
    final private SchemaField field;
    final private DocRouter.Range[] rangesArr;
    final private DocRouter docRouter;
    final private String splitKey;

    ShardSplitingQuery(int partition, SchemaField field, DocRouter.Range[] rangesArr, DocRouter docRouter, String splitKey) {
      this.partition = partition;
      this.field = field;
      this.rangesArr = rangesArr;
      this.docRouter = docRouter;
      this.splitKey = splitKey;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      return new ConstantScoreWeight(this, boost) {

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          FixedBitSet set = findDocsToDelete(context);
          log.info("### partition=" + partition + ", leaf=" + context + ", maxDoc=" + context.reader().maxDoc() +
          ", numDels=" + context.reader().numDeletedDocs() + ", setLen=" + set.length() + ", setCard=" + set.cardinality());
          Bits liveDocs = context.reader().getLiveDocs();
          if (liveDocs != null) {
            // check that we don't delete already deleted docs
            FixedBitSet dels = FixedBitSet.copyOf(liveDocs);
            dels.flip(0, dels.length());
            dels.and(set);
            if (dels.cardinality() > 0) {
              log.error("### INVALID DELS " + dels.cardinality());
            }
          }
          return new ConstantScoreScorer(this, score(), new BitSetIterator(set, set.length()));
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return false;
        }

        @Override
        public String toString() {
          return "weight(shardSplittingQuery,part" + partition + ")";
        }
      };
    }

    private FixedBitSet findDocsToDelete(LeafReaderContext readerContext) throws IOException {
      LeafReader reader = readerContext.reader();
      FixedBitSet docSet = new FixedBitSet(reader.maxDoc());
      Bits liveDocs = reader.getLiveDocs();

      Terms terms = reader.terms(field.getName());
      TermsEnum termsEnum = terms==null ? null : terms.iterator();
      if (termsEnum == null) return docSet;

      BytesRef term = null;
      PostingsEnum postingsEnum = null;
      HashBasedRouter hashRouter = docRouter instanceof HashBasedRouter ? (HashBasedRouter)docRouter : null;

      CharsRefBuilder idRef = new CharsRefBuilder();
      for (;;) {
        term = termsEnum.next();
        if (term == null) break;

        // figure out the hash for the term

        // FUTURE: if conversion to strings costs too much, we could
        // specialize and use the hash function that can work over bytes.
        field.getType().indexedToReadable(term, idRef);
        String idString = idRef.toString();

        if (splitKey != null) {
          // todo have composite routers support these kind of things instead
          String part1 = getRouteKey(idString);
          if (part1 == null)
            continue;
          if (!splitKey.equals(part1))  {
            continue;
          }
        }

        int hash = 0;
        if (hashRouter != null && rangesArr != null) {
          hash = hashRouter.sliceHash(idString, null, null, null);
        }

        postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
        postingsEnum = BitsFilteredPostingsEnum.wrap(postingsEnum, liveDocs);
        for (;;) {
          int doc = postingsEnum.nextDoc();
          if (doc == DocIdSetIterator.NO_MORE_DOCS) break;
          if (rangesArr == null) {
            if (doc % partition != 0) {
              docSet.set(doc);
            }
          } else  {
            if (!rangesArr[partition].includes(hash)) {
              docSet.set(doc);
            }
          }
        }
      }
      return docSet;
    }

    @Override
    public String toString(String field) {
      return "shardSplittingQuery";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      ShardSplitingQuery q = (ShardSplitingQuery)obj;
      if (partition != q.partition) {
        return false;
      }
      return true;
    }

    @Override
    public int hashCode() {
      return partition;
    }
  }

  FixedBitSet[] split(LeafReaderContext readerContext) throws IOException {
    LeafReader reader = readerContext.reader();
    FixedBitSet[] docSets = new FixedBitSet[numPieces];
    for (int i=0; i<docSets.length; i++) {
      docSets[i] = new FixedBitSet(reader.maxDoc());
    }
    Bits liveDocs = reader.getLiveDocs();

    Terms terms = reader.terms(field.getName());
    TermsEnum termsEnum = terms==null ? null : terms.iterator();
    if (termsEnum == null) return docSets;

    BytesRef term = null;
    PostingsEnum postingsEnum = null;

    int[] docsMatchingRanges = null;
    if (ranges != null) {
      // +1 because documents can belong to *zero*, one, several or all ranges in rangesArr
      docsMatchingRanges = new int[rangesArr.length+1];
    }

    CharsRefBuilder idRef = new CharsRefBuilder();
    for (;;) {
      term = termsEnum.next();
      if (term == null) break;

      // figure out the hash for the term

      // FUTURE: if conversion to strings costs too much, we could
      // specialize and use the hash function that can work over bytes.
      field.getType().indexedToReadable(term, idRef);
      String idString = idRef.toString();

      if (splitKey != null) {
        // todo have composite routers support these kind of things instead
        String part1 = getRouteKey(idString);
        if (part1 == null)
          continue;
        if (!splitKey.equals(part1))  {
          continue;
        }
      }

      int hash = 0;
      if (hashRouter != null) {
        hash = hashRouter.sliceHash(idString, null, null, null);
      }

      postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
      postingsEnum = BitsFilteredPostingsEnum.wrap(postingsEnum, liveDocs);
      for (;;) {
        int doc = postingsEnum.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) break;
        if (ranges == null) {
          docSets[currPartition].set(doc);
          currPartition = (currPartition + 1) % numPieces;
        } else  {
          int matchingRangesCount = 0;
          for (int i=0; i<rangesArr.length; i++) {      // inner-loop: use array here for extra speed.
            if (rangesArr[i].includes(hash)) {
              docSets[i].set(doc);
              ++matchingRangesCount;
            }
          }
          docsMatchingRanges[matchingRangesCount]++;
        }
      }
    }

    if (docsMatchingRanges != null) {
      for (int ii = 0; ii < docsMatchingRanges.length; ii++) {
        if (0 == docsMatchingRanges[ii]) continue;
        switch (ii) {
          case 0:
            // document loss
            log.error("Splitting {}: {} documents belong to no shards and will be dropped",
                reader, docsMatchingRanges[ii]);
            break;
          case 1:
            // normal case, each document moves to one of the sub-shards
            log.info("Splitting {}: {} documents will move into a sub-shard",
                reader, docsMatchingRanges[ii]);
            break;
          default:
            // document duplication
            log.error("Splitting {}: {} documents will be moved to multiple ({}) sub-shards",
                reader, docsMatchingRanges[ii], ii);
            break;
        }
      }
    }

    return docSets;
  }

  public static String getRouteKey(String idString) {
    int idx = idString.indexOf(CompositeIdRouter.SEPARATOR);
    if (idx <= 0) return null;
    String part1 = idString.substring(0, idx);
    int commaIdx = part1.indexOf(CompositeIdRouter.bitsSeparator);
    if (commaIdx > 0) {
      if (commaIdx + 1 < part1.length())  {
        char ch = part1.charAt(commaIdx + 1);
        if (ch >= '0' && ch <= '9') {
          part1 = part1.substring(0, commaIdx);
        }
      }
    }
    return part1;
  }


  // change livedocs on the reader to delete those docs we don't want
  static class LiveDocsReader extends FilterCodecReader {
    final FixedBitSet liveDocs;
    final int numDocs;

    public LiveDocsReader(CodecReader in, FixedBitSet liveDocs) throws IOException {
      super(in);
      this.liveDocs = liveDocs;
      this.numDocs = liveDocs.cardinality();
    }

    @Override
    public int numDocs() {
      return numDocs;
    }

    @Override
    public Bits getLiveDocs() {
      return liveDocs;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

}
