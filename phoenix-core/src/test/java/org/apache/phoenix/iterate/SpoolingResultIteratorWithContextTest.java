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


import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.end2end.BaseClientManagedTimeIT;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.memory.DelegatingMemoryManager;
import org.apache.phoenix.memory.GlobalMemoryManager;
import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.AssertResults;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;


public class SpoolingResultIteratorWithContextTest extends BaseClientManagedTimeIT {

	private final static byte[] A = Bytes.toBytes("a");
	private final static byte[] B = Bytes.toBytes("b");

	private void testSpooling(int threshold, long maxSizeSpool, StatementContext context) throws Throwable {
		Tuple[] results = new Tuple[] {
				new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
				new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
		};
		PeekingResultIterator iterator = new MaterializedResultIterator(Arrays.asList(results));

		Tuple[] expectedResults = new Tuple[] {
				new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
				new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
		};

		MemoryManager memoryManager = new DelegatingMemoryManager(new GlobalMemoryManager(threshold, 0));
		ResultIterator scanner = new SpoolingResultIterator(context, iterator, memoryManager, threshold, maxSizeSpool,"/tmp");
		AssertResults.assertResults(scanner, expectedResults);
	}

	private static HBaseTestingUtility hbaseTestUtil;
	private static PhoenixTestDriver driver;
	private static String url;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		setUpConfigForMiniCluster(conf);
		hbaseTestUtil = new HBaseTestingUtility(conf);
		hbaseTestUtil.startMiniCluster();
		String clientPort = hbaseTestUtil.getConfiguration().get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
		url = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_SEPARATOR + clientPort
				+ JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
		Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
		props.put(QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, Integer.toString(1));
		setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
	}

	@AfterClass
	public static void tearDown() throws Exception {
		try {
			destroyDriver(driver);
		} finally {
			hbaseTestUtil.shutdownMiniCluster();
		}
	} 

	@Test
	public void testDeleteTempFile() throws SQLException, Throwable {
		Properties props = new Properties();
		props.setProperty(QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, Integer.toString(1));
		Connection conn = DriverManager.getConnection(url, props);
		Statement stmt = conn.createStatement();
		stmt.execute("CREATE TABLE test (ID varchar NOT NULL PRIMARY KEY)");
		stmt.execute("UPSERT INTO test VALUES ('each')");
		stmt.execute("UPSERT INTO test VALUES ('name2')");    
		stmt.execute("UPSERT INTO test VALUES ('name3')");    
		stmt.close();
		conn.commit();
		String query = "select * from test";
		conn = DriverManager.getConnection(url, props);
		Statement statement = conn.createStatement();
		statement.execute(query);
		PhoenixStatement pstatement = statement.unwrap(PhoenixStatement.class);
		QueryPlan plan = pstatement.getQueryPlan();
		StatementContext context = plan.getContext();
		// additional test 
		testSpooling(1, QueryServicesOptions.DEFAULT_MAX_SPOOL_TO_DISK_BYTES, context);
		conn.close();
	}

}
