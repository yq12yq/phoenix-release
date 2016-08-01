/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.query.QueryConstants.MILLIS_IN_DAY;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class IndexIT extends BaseHBaseManagedTimeIT {

	private final boolean localIndex;
    private final boolean mutable;
	private final String tableDDLOptions;
	private final String tableName;
    private final String indexName;
    private final String fullTableName;
    private final String fullIndexName;

	public IndexIT(boolean localIndex, boolean mutable) {
		this.localIndex = localIndex;
		this.mutable = mutable;
		StringBuilder optionBuilder = new StringBuilder();
		if (!mutable) {
			optionBuilder.append(" IMMUTABLE_ROWS=true ");
		}
		this.tableDDLOptions = optionBuilder.toString();
		this.tableName = TestUtil.DEFAULT_DATA_TABLE_NAME;
        this.indexName = "IDX";
        this.fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        this.fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
	}

	@BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

	@Parameters(name="localIndex = {0} , mutable = {1}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                 { false, false }, { false, true },
                 { true, false }, { true, true }, { true, true }
           });
    }

    /**
     * Ensure that HTD contains table priorities correctly.
     */
    @Test
    public void testTableDescriptorPriority() throws SQLException, IOException {
      // Check system tables priorities.
      try (HBaseAdmin admin = driver.getConnectionQueryServices(null, null).getAdmin()) {
        for (HTableDescriptor htd : admin.listTables()) {
          if (htd.getTableName().getNameAsString().startsWith(QueryConstants.SYSTEM_SCHEMA_NAME)) {
            String val = htd.getValue("PRIORITY");
            assertNotNull("PRIORITY is not set for table:" + htd, val);
            assertTrue(Integer.parseInt(val)
              >= PhoenixRpcSchedulerFactory.getMetadataPriority(config));
          }
        }

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl ="CREATE TABLE " + fullTableName + BaseTest.TEST_TABLE_SCHEMA + tableDDLOptions;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
          conn.setAutoCommit(false);
          Statement stmt = conn.createStatement();
          stmt.execute(ddl);
          BaseTest.populateTestTable(fullTableName);
          ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName
              + " ON " + fullTableName + " (long_col1, long_col2)"
              + " INCLUDE (decimal_col1, decimal_col2)";
          stmt.execute(ddl);
        }

        HTableDescriptor dataTable = admin.getTableDescriptor(
          org.apache.hadoop.hbase.TableName.valueOf(fullTableName));
        String val = dataTable.getValue("PRIORITY");
        assertTrue(val == null || Integer.parseInt(val) < HConstants.HIGH_QOS);

        if (!localIndex) {
          HTableDescriptor indexTable = admin.getTableDescriptor(
            org.apache.hadoop.hbase.TableName.valueOf(indexName));
          val = indexTable.getValue("PRIORITY");
          assertNotNull("PRIORITY is not set for table:" + indexTable, val);
          assertTrue(Integer.parseInt(val) >= PhoenixRpcSchedulerFactory.getIndexPriority(config));
        }
      }
    }

}
