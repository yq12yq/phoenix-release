/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;
import java.util.UUID;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.junit.Assert.fail;

public class PartialScannerResultsDisabledIT {
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    private static String url;

    @Before
    public void setup() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        hbaseTestUtil = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        hbaseTestUtil.startMiniCluster();
        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
    }
    
    /*
            Can't really assert this on the patch branch since Scrutiny is a newer major feature
            that customer didn't ask for.

    @Test
    public void testWithEnoughData() throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            // Write enough data to trigger partial scanner results
            // TODO: it's likely that less data could be written if whatever
            // config parameters decide this are lowered.
            writeSingleBatch(conn, 100, 20, dataTableFullName);

            logger.info("Running scrutiny");
            // Scutunize index to see if partial results are silently returned
            // In that case we'll get a false positive on the scrutiny run.
            long rowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(2000,rowCount);
        }
    }
    */

    /**
     * Simple select query with fetch size that exceed the result size. In that case scan would start to produce
     * partial result sets that from Phoenix perspective are the rows with NULL values.
     * @throws SQLException
     */
    @Test
    public void partialResultDuringSelect () throws SQLException {
        String tableName = RandomStringUtils.randomAlphabetic(20).toUpperCase();
        Properties props = new Properties();
        props.setProperty(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, "5");
        int numRecords = 10;
        try (Connection conn = DriverManager.getConnection(url, props)) {
            conn.createStatement().execute("CREATE TABLE " + tableName +
                    " (PK1 INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR)");
            int i = 0;
            String upsert = "UPSERT INTO " + tableName + " VALUES (?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            while (i < numRecords) {
                stmt.setInt(1, i);
                stmt.setString(2, UUID.randomUUID().toString());
                stmt.executeUpdate();
                i++;
            }
            conn.commit();

            String sql = "SELECT * FROM " + tableName;
            // at every next call wait for this period. This will cause lease to expire.
            Statement s = conn.createStatement();
            s.setFetchSize(100);
            ResultSet rs = s.executeQuery(sql);
            while (rs.next()) {
                if (rs.getString(2) == null)
                    fail("Null value because of partial row scan");
            }
        }
    }
}
