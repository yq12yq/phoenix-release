/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index.metrics;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * Implementation for tracking Phoenix Indexer metrics.
 */
public class MetricsIndexerSourceImpl extends BaseSourceImpl implements MetricsIndexerSource {

    private final MetricHistogram preBatchMutateTimeHisto;
    private final MutableCounterLong slowPreBatchMutateCalls;
    private final MetricHistogram postBatchMutateTimeHisto;
    private final MutableCounterLong slowPostBatchMutateCalls;
    private final MetricHistogram postBatchMutateIndispenablyTimeHisto;
    private final MutableCounterLong slowPostBatchMutateIndispensablyCalls;
    private final MetricHistogram preWALRestoreTimeHisto;
    private final MutableCounterLong slowPreWALRestoreCalls;
    private final MetricHistogram postPutTimeHisto;
    private final MutableCounterLong slowPostPutCalls;
    private final MetricHistogram postDeleteTimeHisto;
    private final MutableCounterLong slowPostDeleteCalls;
    private final MetricHistogram postOpenTimeHisto;
    private final MutableCounterLong slowPostOpenCalls;

    public MetricsIndexerSourceImpl() {
        this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
    }

    public MetricsIndexerSourceImpl(String metricsName, String metricsDescription,
        String metricsContext, String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

        preBatchMutateTimeHisto = getMetricsRegistry().newHistogram(PRE_BATCH_MUTATE_TIME, PRE_BATCH_MUTATE_TIME_DESC);
        slowPreBatchMutateCalls = getMetricsRegistry().newCounter(SLOW_PRE_BATCH_MUTATE, SLOW_PRE_BATCH_MUTATE_DESC, 0L);
        postBatchMutateTimeHisto = getMetricsRegistry().newHistogram(POST_BATCH_MUTATE_TIME, POST_BATCH_MUTATE_TIME_DESC);
        slowPostBatchMutateCalls = getMetricsRegistry().newCounter(SLOW_POST_BATCH_MUTATE, SLOW_POST_BATCH_MUTATE_DESC, 0L);
        postBatchMutateIndispenablyTimeHisto = getMetricsRegistry().newHistogram(POST_BATCH_MUTATE_INDISPENSABLY_TIME, POST_BATCH_MUTATE_INDISPENSABLY_TIME_DESC);
        slowPostBatchMutateIndispensablyCalls = getMetricsRegistry().newCounter(SLOW_POST_BATCH_MUTATE_INDISPENSABLY, SLOW_POST_BATCH_MUTATE_INDISPENSABLY_DESC, 0L);
        preWALRestoreTimeHisto = getMetricsRegistry().newHistogram(PRE_WAL_RESTORE_TIME, PRE_WAL_RESTORE_TIME_DESC);
        slowPreWALRestoreCalls = getMetricsRegistry().newCounter(SLOW_PRE_WAL_RESTORE, SLOW_PRE_WAL_RESTORE_DESC, 0L);
        postPutTimeHisto = getMetricsRegistry().newHistogram(POST_PUT_TIME, POST_PUT_TIME_DESC);
        slowPostPutCalls = getMetricsRegistry().newCounter(SLOW_POST_PUT, SLOW_POST_PUT_DESC, 0L);
        postDeleteTimeHisto = getMetricsRegistry().newHistogram(POST_DELETE_TIME, POST_DELETE_TIME_DESC);
        slowPostDeleteCalls = getMetricsRegistry().newCounter(SLOW_POST_DELETE, SLOW_POST_DELETE_DESC, 0L);
        postOpenTimeHisto = getMetricsRegistry().newHistogram(POST_OPEN_TIME, POST_OPEN_TIME_DESC);
        slowPostOpenCalls = getMetricsRegistry().newCounter(SLOW_POST_OPEN, SLOW_POST_OPEN_DESC, 0L);
    }

    @Override
    public void updatePreBatchMutateTime(long t) {
        preBatchMutateTimeHisto.add(t);
    }

    @Override
    public void updatePostBatchMutateTime(long t) {
        postBatchMutateTimeHisto.add(t);
    }

    @Override
    public void updatePostBatchMutateIndispensablyTime(long t) {
        postBatchMutateIndispenablyTimeHisto.add(t);
    }

    @Override
    public void updatePreWALRestoreTime(long t) {
        preWALRestoreTimeHisto.add(t);
    }

    @Override
    public void updatePostPutTime(long t) {
        postPutTimeHisto.add(t);
    }

    @Override
    public void updatePostDeleteTime(long t) {
        postDeleteTimeHisto.add(t);
    }

    @Override
    public void updatePostOpenTime(long t) {
        postOpenTimeHisto.add(t);
    }

    @Override
    public void incrementNumSlowPreBatchMutateCalls() {
        slowPreBatchMutateCalls.incr();
    }

    @Override
    public void incrementNumSlowPostBatchMutateCalls() {
        slowPostBatchMutateCalls.incr();
    }

    @Override
    public void incrementNumSlowPostBatchMutateIndispensablyCalls() {
        slowPostBatchMutateIndispensablyCalls.incr();
    }

    @Override
    public void incrementNumSlowPreWALRestoreCalls() {
        slowPreWALRestoreCalls.incr();
    }

    @Override
    public void incrementNumSlowPostPutCalls() {
        slowPostPutCalls.incr();
    }

    @Override
    public void incrementNumSlowPostDeleteCalls() {
        slowPostDeleteCalls.incr();
    }

    @Override
    public void incrementNumSlowPostOpenCalls() {
        slowPostOpenCalls.incr();
    }
}
