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

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.phoenix.hbase.index.Indexer;

/**
 * Interface for metrics about {@link Indexer}.
 */
public interface MetricsIndexerSource extends BaseSource {

  // Metrics2 and JMX constants
  String METRICS_NAME = "PhoenixIndexer";
  String METRICS_CONTEXT = "phoenix";
  String METRICS_DESCRIPTION = "Metrics about the Phoenix Indexer";
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  String PRE_BATCH_MUTATE_TIME = "preBatchMutateTime";
  String PRE_BATCH_MUTATE_TIME_DESC = "Histogram for the time in milliseconds for Indexer's preBatchMutate";
  String SLOW_PRE_BATCH_MUTATE = "slowPreBatchMutateCalls";
  String SLOW_PRE_BATCH_MUTATE_DESC = "The number of preBatchMutate calls slower than the configured threshold";

  String POST_BATCH_MUTATE_TIME = "postBatchMutateTime";
  String POST_BATCH_MUTATE_TIME_DESC = "Histogram for the time in milliseconds for Indexer's postBatchMutate";
  String SLOW_POST_BATCH_MUTATE = "slowPostBatchMutateCalls";
  String SLOW_POST_BATCH_MUTATE_DESC = "The number of postBatchMutate calls slower than the configured threshold";

  String POST_BATCH_MUTATE_INDISPENSABLY_TIME = "postBatchMutateIndispensablyTime";
  String POST_BATCH_MUTATE_INDISPENSABLY_TIME_DESC = "Histogram for the time in milliseconds for Indexer's postBatchMutateIndispsenably";
  String SLOW_POST_BATCH_MUTATE_INDISPENSABLY = "slowPostBatchMutateIndispensablyCalls";
  String SLOW_POST_BATCH_MUTATE_INDISPENSABLY_DESC = "The number of postBatchMutateIndispensably calls slower than the configured threshold";

  String PRE_WAL_RESTORE_TIME = "preWALRestoreTime";
  String PRE_WAL_RESTORE_TIME_DESC = "Histogram for the time in milliseconds for Indexer's preWALRestore";
  String SLOW_PRE_WAL_RESTORE = "slowPreWALRestoreCalls";
  String SLOW_PRE_WAL_RESTORE_DESC = "The number of preWALRestore calls slower than the configured threshold";

  String POST_PUT_TIME = "postPutTime";
  String POST_PUT_TIME_DESC = "Histogram for the time in milliseconds for Indexer's postPut";
  String SLOW_POST_PUT = "slowPostPutCalls";
  String SLOW_POST_PUT_DESC = "The number of postPut calls slower than the configured threshold";

  String POST_DELETE_TIME = "postDeleteTime";
  String POST_DELETE_TIME_DESC = "Histogram for the time in milliseconds for Indexer's postDelete";
  String SLOW_POST_DELETE = "slowPostDeleteCalls";
  String SLOW_POST_DELETE_DESC = "The number of postDelete calls slower than the configured threshold";

  String POST_OPEN_TIME = "postOpenTime";
  String POST_OPEN_TIME_DESC = "Histogram for the time in milliseconds for Indexer's postOpen";
  String SLOW_POST_OPEN = "slowPostOpenCalls";
  String SLOW_POST_OPEN_DESC = "The number of postOpen calls slower than the configured threshold";

  /**
   * Updates the preBatchMutate time histogram.
   *
   * @param t time taken in milliseconds
   */
  void updatePreBatchMutateTime(long t);

  /**
   * Increments the number of slow preBatchMutate calls.
   */
  void incrementNumSlowPreBatchMutateCalls();

  /**
   * Updates the postBatchMutate time histogram.
   *
   * @param t time taken in milliseconds
   */
  void updatePostBatchMutateTime(long t);

  /**
   * Increments the number of slow postBatchMutate calls.
   */
  void incrementNumSlowPostBatchMutateCalls();

  /**
   * Updates the postBatchMutateIndispensibly time histogram.
   *
   * @param t time taken in milliseconds
   */
  void updatePostBatchMutateIndispensablyTime(long t);

  /**
   * Increments the number of slow postBatchMutateIndispensably calls.
   */
  void incrementNumSlowPostBatchMutateIndispensablyCalls();

  /**
   * Updates the preWALRestore time histogram.
   *
   * @param t time taken in milliseconds
   */
  void updatePreWALRestoreTime(long t);

  /**
   * Increments the number of slow preWALRestore calls.
   */
  void incrementNumSlowPreWALRestoreCalls();

  /**
   * Updates the postPut time histogram.
   *
   * @param t time taken in milliseconds
   */
  void updatePostPutTime(long t);

  /**
   * Increments the number of slow postPut calls.
   */
  void incrementNumSlowPostPutCalls();

  /**
   * Updates the postDelete time histogram.
   *
   * @param t time taken in milliseconds
   */
  void updatePostDeleteTime(long t);

  /**
   * Increments the number of slow postDelete calls.
   */
  void incrementNumSlowPostDeleteCalls();

  /**
   * Updates the postOpen time histogram.
   *
   * @param t time taken in milliseconds
   */
  void updatePostOpenTime(long t);

  /**
   * Increments the number of slow postOpen calls.
   */
  void incrementNumSlowPostOpenCalls();
}
