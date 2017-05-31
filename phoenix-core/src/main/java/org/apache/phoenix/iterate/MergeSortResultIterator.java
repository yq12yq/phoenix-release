/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.iterate;

import java.nio.MappedByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ResultUtil;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.ServerUtil;

import com.google.common.collect.Lists;


/**
 * 
 * Base class for a ResultIterator that does a merge sort on the list of iterators
 * provided.
 *
 * 
 * @since 1.2
 */
public abstract class MergeSortResultIterator implements PeekingResultIterator {
    protected final ResultIterators resultIterators;
    protected final ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();
    private List<PeekingResultIterator> iterators;
    PeekingResultIterator mergedIterator = null;
    private StatementContext context;
    private static final Log LOG = LogFactory.getLog(MergeSortResultIterator.class);

    public MergeSortResultIterator(ResultIterators iterators,StatementContext context) {
        this.resultIterators = iterators;
        this.context=context;
        if(context==null){
            LOG.debug("Falling back to single threaded sort as context is found to be null");
        }
    }

    private List<PeekingResultIterator> getIterators() throws SQLException {
        if (iterators == null) {
            iterators = resultIterators.getIterators();
            LOG.debug("Number of result Iterators: "+iterators.size());
        }
        return iterators;
    }

    @Override
    public void close() throws SQLException {
        SQLException toThrow = null;
        try {
            if (resultIterators != null) {
                resultIterators.close();
            }

        } catch (Exception e) {
            toThrow = ServerUtil.parseServerException(e);
        } finally {
            try {
                if (iterators != null) {
                    SQLCloseables.closeAll(iterators);
                }
            } catch (Exception e) {
                if (toThrow == null) {
                    toThrow = ServerUtil.parseServerException(e);
                } else {
                    toThrow.setNextException(ServerUtil.parseServerException(e));
                }
            } finally {
                if (toThrow != null) {
                    throw toThrow;
                }
            }
        }
    }

    class MergeIterators
    {
        private final int nThreads = getNumberOfThreads();
        public PeekingResultIterator mergeIteratorsWithThreads(List<PeekingResultIterator> iterators,StatementContext context,int thresholdBytes) throws SQLException{
            LOG.debug("Starting merge sort using heap...");
            Long startTime=System.currentTimeMillis();
            List<Future<PeekingResultIterator>> list = new ArrayList<Future<PeekingResultIterator>>();
            ExecutorService executor = context.getConnection().getQueryServices().getExecutor();
            LOG.debug("Using nThreads:"+nThreads+" for merge sort");
            Map<Integer,List<PeekingResultIterator>> buckets=prepareBuckets(iterators);
            final int thresholdBytesFinal=thresholdBytes;
            for (int i = 0; i < buckets.size(); i++) {
                final List<PeekingResultIterator> mergeIteratorList=buckets.get(i);    
                Callable<PeekingResultIterator> worker = new  Callable<PeekingResultIterator>() {
                    @Override
                    public PeekingResultIterator call() throws SQLException {
                        return mergeIterators(mergeIteratorList,thresholdBytesFinal);
                    }

                };
                Future<PeekingResultIterator> submit= executor.submit(worker);
                list.add(submit);
            }

            List<PeekingResultIterator> resultList = new ArrayList<PeekingResultIterator>();
            for (Future<PeekingResultIterator> future : list) {
                try {
                    resultList.add(future.get());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            if (list.size()!=resultList.size()){
                throw new RuntimeException("Double-entries!!!"); 
            }
            PeekingResultIterator mergeIterators = mergeIterators(resultList,thresholdBytes);
            Long endTime=System.currentTimeMillis();
            Long timeTaken=endTime-startTime;
            LOG.debug("Total time taken for merge sort:"+timeTaken);
            LOG.debug("Merge sort completed!!");
            return mergeIterators;   

        }
        private Map<Integer, List<PeekingResultIterator>> prepareBuckets(List<PeekingResultIterator> iterators) {
            Map<Integer, List<PeekingResultIterator>> buckets=new HashMap<Integer, List<PeekingResultIterator>>();
            int k=0;
            for(int i=0;i<iterators.size();i++){
                if(k>=nThreads){
                    k=0;
                }
                List<PeekingResultIterator> list = buckets.get(k);
                if(list==null){
                    list=new ArrayList<PeekingResultIterator>();
                    buckets.put(k, list);
                }
                list.add(iterators.get(i));
                k++;
            }
            LOG.debug("Number of buckets:"+buckets.size());
            return buckets;
        }
        public  PeekingResultIterator mergeIterators(List<PeekingResultIterator> iterators,int thresholdBytes) throws SQLException
        {

            PriorityQueue < IteratorContainer > heap=new PriorityQueue < IteratorContainer >();

            for(int i=0;i < iterators.size();++i)
            {
                IteratorContainer iteratorContainer = new IteratorContainer(iterators.get(i));
                if (iteratorContainer.isNull())
                    continue;
                heap.add(iteratorContainer);
            }
            final MappedByteBufferTupleQueue queueEntries = new MappedByteBufferTupleQueue(thresholdBytes);
            while(!heap.isEmpty())
            {
                IteratorContainer iteratorContainer=heap.poll();
                queueEntries.add(iteratorContainer.next());
                if (iteratorContainer.isNull()){
                    iteratorContainer.close();
                    continue;
                }
                heap.add(iteratorContainer);
            }
            return new MaterializedResultIterator(queueEntries);
        }


        private  class IteratorContainer implements Comparable <IteratorContainer>
        {
            private PeekingResultIterator iterator;
            public IteratorContainer(PeekingResultIterator iterator)
            {
                this.iterator=iterator;
            }
            public Tuple next() throws SQLException{
                return iterator.next();
            }
            public boolean isNull() throws SQLException
            {
                return iterator.peek()==null;
            }
            public Tuple peek() throws SQLException{
                return iterator.peek();
            }
            public void close() throws SQLException{
                 iterator.close();
            }

            @Override
            public int compareTo(IteratorContainer o)
            {
                try {
                    return compare(iterator.peek(),o.peek());
                } catch (SQLException e) {
                    return 0;
                }
            }

        }
    }

    /*
     * Optimized singleThreadedMinIterator works best when sort is not requested on aggregated value and there are limits used in the query.
     */
    private PeekingResultIterator singleThreadedMinIterator() throws SQLException {

        List<PeekingResultIterator> iterators = getIterators();
        Tuple minResult = null;
        PeekingResultIterator minIterator = EMPTY_ITERATOR;
        for (int i = iterators.size()-1; i >= 0; i--) {
            PeekingResultIterator iterator = iterators.get(i);
            Tuple r = iterator.peek();
            if (r != null) {
                if (minResult == null || compare(r, minResult) < 0) {
                    minResult = r;
                    minIterator = iterator;
                }
                continue;
            }
            iterator.close();
            iterators.remove(i);
        }
        return minIterator;
    }

    /**
     * 
     * @return minIterator in case of single threaded mergesort and return mergedSortedIterator in case of multi-threaded sort 
     * @throws SQLException
     */
    private PeekingResultIterator minIterator() throws SQLException {
        if(this.mergedIterator==null && !isSerial() ){
            MergeIterators mI=new MergeIterators();
            int thresholdBytes = context.getConnection().getQueryServices().getProps().getInt(
                QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES);
          
            this.mergedIterator = mI.mergeIteratorsWithThreads(getIterators(),context,thresholdBytes);
        }else if(isSerial()){
            return singleThreadedMinIterator();
        }
        return mergedIterator;
    }

    private boolean isSerial(){
        int thread=getNumberOfThreads();
        // if context is not available, then falling back on singleThreaded optimized for limit
        return thread<=1 || context==null;
    }

    /**
     * Implement to tell how the records needs to be compared during merge sort.
     * @param t1 tuple1
     * @param t2 tuple2
     * @return
     */
    abstract protected int compare(Tuple t1, Tuple t2);

    /**
     * 
     * @return Implementation should return number of threads needs to be used for merge sort
     *
     */
    abstract public int getNumberOfThreads();

    @Override
    public Tuple peek() throws SQLException {
        PeekingResultIterator iterator=minIterator();
        return iterator.peek();
    }

    @Override
    public Tuple next() throws SQLException {
        PeekingResultIterator iterator = minIterator();
        return iterator.next();
    }
    private static class MappedByteBufferTupleQueue extends MappedByteBufferQueue<Tuple> {

        public MappedByteBufferTupleQueue(int thresholdBytes) {
            super(thresholdBytes);
        }

        @Override
        protected MappedByteBufferSegmentQueue<Tuple> createSegmentQueue(
                int index, int thresholdBytes) {
            return new MappedByteBufferTupleSegmentQueue(index, thresholdBytes, false);
        }

        @Override
        protected Comparator<MappedByteBufferSegmentQueue<Tuple>> getSegmentQueueComparator() {
            return new Comparator<MappedByteBufferSegmentQueue<Tuple>>() {
                @Override
                public int compare(MappedByteBufferSegmentQueue<Tuple> q1, 
                        MappedByteBufferSegmentQueue<Tuple> q2) {
                    return q1.index() - q2.index();
                }                
            };
        }

        @Override
        public Iterator<Tuple> iterator() {
            return new Iterator<Tuple>() {
                private Iterator<MappedByteBufferSegmentQueue<Tuple>> queueIter;
                private Iterator<Tuple> currentIter;
                {
                    this.queueIter = getSegmentQueues().iterator();
                    this.currentIter = queueIter.hasNext() ? queueIter.next().iterator() : null;
                }
                
                @Override
                public boolean hasNext() {
                    return currentIter != null && currentIter.hasNext();
                }

                @Override
                public Tuple next() {
                    if (!hasNext())
                        return null;
                    
                    Tuple ret = currentIter.next();                    
                    if (!currentIter.hasNext()) {
                        this.currentIter = queueIter.hasNext() ? queueIter.next().iterator() : null;                       
                    }
                    
                    return ret;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
                
            };
        }
        
        private static class MappedByteBufferTupleSegmentQueue extends MappedByteBufferSegmentQueue<Tuple> {
            private LinkedList<Tuple> results;
            
            public MappedByteBufferTupleSegmentQueue(int index,
                    int thresholdBytes, boolean hasMaxQueueSize) {
                super(index, thresholdBytes, hasMaxQueueSize);
                this.results = Lists.newLinkedList();
            }

            @Override
            protected Queue<Tuple> getInMemoryQueue() {
                return results;
            }

            @Override
            protected int sizeOf(Tuple e) {
                KeyValue kv = KeyValueUtil.ensureKeyValue(e.getValue(0));
                return Bytes.SIZEOF_INT * 2 + kv.getLength();
            }

            @SuppressWarnings("deprecation")
            @Override
            protected void writeToBuffer(MappedByteBuffer buffer, Tuple e) {
                KeyValue kv = KeyValueUtil.ensureKeyValue(e.getValue(0));
                buffer.putInt(kv.getLength() + Bytes.SIZEOF_INT);
                buffer.putInt(kv.getLength());
                buffer.put(kv.getBuffer(), kv.getOffset(), kv.getLength());
            }

            @Override
            protected Tuple readFromBuffer(MappedByteBuffer buffer) {
                int length = buffer.getInt();
                if (length < 0)
                    return null;
                
                byte[] b = new byte[length];
                buffer.get(b);
                Result result = ResultUtil.toResult(new ImmutableBytesWritable(b));
                return new ResultTuple(result);
            }
            
        }
    }


}
