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

package org.apache.phoenix.util.repairtool.modules;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.repairtool.ConsoleUI;
import org.apache.phoenix.util.repairtool.utils.HBaseUtils;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.util.*;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

import static org.apache.hadoop.hbase.KeyValueUtil.createFirstOnRow;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static org.apache.phoenix.schema.PTableType.INDEX;
import static org.apache.phoenix.schema.PTableType.TABLE;
import static org.apache.phoenix.util.SchemaUtil.getVarCharLength;

/**
 *
 * The way how consistancy check works:
 * 1. We scan SYSTEM.CATALOG for table records using QUALIFIER filter and collect them.
 * 2. We check every table record for correctness:
 *    a. Read it using modified MetaDataEndopoint.getTable
 *    b. Check that physical table exist and enabled
 *    c. Read columns and check that number of columns matches in meta, check that column positions are correct
 * This class is copying the stuff from MetaDataEndpoint impl with additional checks
 */
public class SystemCatalogCheck {


    public static final String ROW_KEY_ORDER_OPTIMIZABLE = "ROW_KEY_ORDER_OPTIMIZABLE";
    public static final byte[] ROW_KEY_ORDER_OPTIMIZABLE_BYTES = Bytes.toBytes(ROW_KEY_ORDER_OPTIMIZABLE);

    // KeyValues for Table
    private static final KeyValue TABLE_TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES);
    private static final KeyValue TABLE_SEQ_NUM_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TABLE_SEQ_NUM_BYTES);
    private static final KeyValue COLUMN_COUNT_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, COLUMN_COUNT_BYTES);
    private static final KeyValue SALT_BUCKETS_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, SALT_BUCKETS_BYTES);
    private static final KeyValue PK_NAME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, PK_NAME_BYTES);
    private static final KeyValue DATA_TABLE_NAME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DATA_TABLE_NAME_BYTES);
    private static final KeyValue INDEX_STATE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, INDEX_STATE_BYTES);
    private static final KeyValue IMMUTABLE_ROWS_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, IMMUTABLE_ROWS_BYTES);
    private static final KeyValue VIEW_EXPRESSION_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, VIEW_STATEMENT_BYTES);
    private static final KeyValue DEFAULT_COLUMN_FAMILY_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DEFAULT_COLUMN_FAMILY_NAME_BYTES);
    private static final KeyValue DISABLE_WAL_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DISABLE_WAL_BYTES);
    private static final KeyValue MULTI_TENANT_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, MULTI_TENANT_BYTES);
    private static final KeyValue VIEW_TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, VIEW_TYPE_BYTES);
    private static final KeyValue VIEW_INDEX_ID_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, VIEW_INDEX_ID_BYTES);
    private static final KeyValue INDEX_TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, INDEX_TYPE_BYTES);
    private static final KeyValue INDEX_DISABLE_TIMESTAMP_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, INDEX_DISABLE_TIMESTAMP_BYTES);
    private static final KeyValue STORE_NULLS_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, STORE_NULLS_BYTES);
    private static final KeyValue ASYNC_REBUILD_TIMESTAMP_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, ASYNC_REBUILD_TIMESTAMP_BYTES);
    private static final KeyValue EMPTY_KEYVALUE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES);
    private static final KeyValue BASE_COLUMN_COUNT_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.BASE_COLUMN_COUNT_BYTES);
    private static final KeyValue ROW_KEY_ORDER_OPTIMIZABLE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, ROW_KEY_ORDER_OPTIMIZABLE_BYTES);
    private static final KeyValue TRANSACTIONAL_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TRANSACTIONAL_BYTES);
    private static final KeyValue UPDATE_CACHE_FREQUENCY_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, UPDATE_CACHE_FREQUENCY_BYTES);
    private static final KeyValue IS_NAMESPACE_MAPPED_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY,
            TABLE_FAMILY_BYTES, IS_NAMESPACE_MAPPED_BYTES);
    //    Following keys will be required in 4.9 Phoenix
    //    private static final KeyValue AUTO_PARTITION_SEQ_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, AUTO_PARTITION_SEQ_BYTES);
    //    private static final KeyValue APPEND_ONLY_SCHEMA_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, APPEND_ONLY_SCHEMA_BYTES);

    private static final List<KeyValue> TABLE_KV_COLUMNS = Arrays.<KeyValue>asList(
            EMPTY_KEYVALUE_KV,
            TABLE_TYPE_KV,
            TABLE_SEQ_NUM_KV,
            COLUMN_COUNT_KV,
            SALT_BUCKETS_KV,
            PK_NAME_KV,
            DATA_TABLE_NAME_KV,
            INDEX_STATE_KV,
            IMMUTABLE_ROWS_KV,
            VIEW_EXPRESSION_KV,
            DEFAULT_COLUMN_FAMILY_KV,
            DISABLE_WAL_KV,
            MULTI_TENANT_KV,
            VIEW_TYPE_KV,
            VIEW_INDEX_ID_KV,
            INDEX_TYPE_KV,
            INDEX_DISABLE_TIMESTAMP_KV,
            STORE_NULLS_KV,
            BASE_COLUMN_COUNT_KV,
            ROW_KEY_ORDER_OPTIMIZABLE_KV,
            TRANSACTIONAL_KV,
            UPDATE_CACHE_FREQUENCY_KV,
            IS_NAMESPACE_MAPPED_KV,
//            AUTO_PARTITION_SEQ_KV,
//            APPEND_ONLY_SCHEMA_KV,
            ASYNC_REBUILD_TIMESTAMP_KV
    );

    static {
        Collections.sort(TABLE_KV_COLUMNS, KeyValue.COMPARATOR);
    }

    private static final int TABLE_TYPE_INDEX = TABLE_KV_COLUMNS.indexOf(TABLE_TYPE_KV);
    private static final int TABLE_SEQ_NUM_INDEX = TABLE_KV_COLUMNS.indexOf(TABLE_SEQ_NUM_KV);
    private static final int COLUMN_COUNT_INDEX = TABLE_KV_COLUMNS.indexOf(COLUMN_COUNT_KV);
    private static final int SALT_BUCKETS_INDEX = TABLE_KV_COLUMNS.indexOf(SALT_BUCKETS_KV);
    private static final int PK_NAME_INDEX = TABLE_KV_COLUMNS.indexOf(PK_NAME_KV);
    private static final int DATA_TABLE_NAME_INDEX = TABLE_KV_COLUMNS.indexOf(DATA_TABLE_NAME_KV);
    private static final int INDEX_STATE_INDEX = TABLE_KV_COLUMNS.indexOf(INDEX_STATE_KV);
    private static final int IMMUTABLE_ROWS_INDEX = TABLE_KV_COLUMNS.indexOf(IMMUTABLE_ROWS_KV);
    private static final int VIEW_STATEMENT_INDEX = TABLE_KV_COLUMNS.indexOf(VIEW_EXPRESSION_KV);
    private static final int DEFAULT_COLUMN_FAMILY_INDEX = TABLE_KV_COLUMNS.indexOf(DEFAULT_COLUMN_FAMILY_KV);
    private static final int DISABLE_WAL_INDEX = TABLE_KV_COLUMNS.indexOf(DISABLE_WAL_KV);
    private static final int MULTI_TENANT_INDEX = TABLE_KV_COLUMNS.indexOf(MULTI_TENANT_KV);
    private static final int VIEW_TYPE_INDEX = TABLE_KV_COLUMNS.indexOf(VIEW_TYPE_KV);
    private static final int VIEW_INDEX_ID_INDEX = TABLE_KV_COLUMNS.indexOf(VIEW_INDEX_ID_KV);
    private static final int INDEX_TYPE_INDEX = TABLE_KV_COLUMNS.indexOf(INDEX_TYPE_KV);
    private static final int STORE_NULLS_INDEX = TABLE_KV_COLUMNS.indexOf(STORE_NULLS_KV);
    private static final int BASE_COLUMN_COUNT_INDEX = TABLE_KV_COLUMNS.indexOf(BASE_COLUMN_COUNT_KV);
    private static final int ROW_KEY_ORDER_OPTIMIZABLE_INDEX = TABLE_KV_COLUMNS.indexOf(ROW_KEY_ORDER_OPTIMIZABLE_KV);
    private static final int TRANSACTIONAL_INDEX = TABLE_KV_COLUMNS.indexOf(TRANSACTIONAL_KV);
    private static final int UPDATE_CACHE_FREQUENCY_INDEX = TABLE_KV_COLUMNS.indexOf(UPDATE_CACHE_FREQUENCY_KV);
    private static final int INDEX_DISABLE_TIMESTAMP = TABLE_KV_COLUMNS.indexOf(INDEX_DISABLE_TIMESTAMP_KV);
    private static final int IS_NAMESPACE_MAPPED_INDEX = TABLE_KV_COLUMNS.indexOf(IS_NAMESPACE_MAPPED_KV);
//    private static final int AUTO_PARTITION_SEQ_INDEX = TABLE_KV_COLUMNS.indexOf(AUTO_PARTITION_SEQ_KV);
//    private static final int APPEND_ONLY_SCHEMA_INDEX = TABLE_KV_COLUMNS.indexOf(APPEND_ONLY_SCHEMA_KV);

    // KeyValues for Column
    private static final KeyValue DECIMAL_DIGITS_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DECIMAL_DIGITS_BYTES);
    private static final KeyValue COLUMN_SIZE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, COLUMN_SIZE_BYTES);
    private static final KeyValue NULLABLE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, NULLABLE_BYTES);
    private static final KeyValue DATA_TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DATA_TYPE_BYTES);
    private static final KeyValue ORDINAL_POSITION_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, ORDINAL_POSITION_BYTES);
    private static final KeyValue SORT_ORDER_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, SORT_ORDER_BYTES);
    private static final KeyValue ARRAY_SIZE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, ARRAY_SIZE_BYTES);
    private static final KeyValue VIEW_CONSTANT_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, VIEW_CONSTANT_BYTES);
    private static final KeyValue IS_VIEW_REFERENCED_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, IS_VIEW_REFERENCED_BYTES);
    private static final KeyValue COLUMN_DEF_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, COLUMN_DEF_BYTES);
    private static final KeyValue IS_ROW_TIMESTAMP_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, IS_ROW_TIMESTAMP_BYTES);
    private static final KeyValue KEY_SEQ_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, KEY_SEQ_BYTES);
    private static final List<KeyValue> COLUMN_KV_COLUMNS = Arrays.<KeyValue>asList(
            DECIMAL_DIGITS_KV,
            COLUMN_SIZE_KV,
            NULLABLE_KV,
            DATA_TYPE_KV,
            KEY_SEQ_KV,
            ORDINAL_POSITION_KV,
            PK_NAME_KV,
            SORT_ORDER_KV,
            DATA_TABLE_NAME_KV, // included in both column and table row for metadata APIs
            ARRAY_SIZE_KV,
            VIEW_CONSTANT_KV,
            IS_VIEW_REFERENCED_KV,
            COLUMN_DEF_KV,
            IS_ROW_TIMESTAMP_KV
    );

    static {
        Collections.sort(COLUMN_KV_COLUMNS, KeyValue.COMPARATOR);
    }

    private static final int DECIMAL_DIGITS_INDEX = COLUMN_KV_COLUMNS.indexOf(DECIMAL_DIGITS_KV);
    private static final int COLUMN_SIZE_INDEX = COLUMN_KV_COLUMNS.indexOf(COLUMN_SIZE_KV);
    private static final int NULLABLE_INDEX = COLUMN_KV_COLUMNS.indexOf(NULLABLE_KV);
    private static final int DATA_TYPE_INDEX = COLUMN_KV_COLUMNS.indexOf(DATA_TYPE_KV);
    private static final int ORDINAL_POSITION_INDEX = COLUMN_KV_COLUMNS.indexOf(ORDINAL_POSITION_KV);
    private static final int SORT_ORDER_INDEX = COLUMN_KV_COLUMNS.indexOf(SORT_ORDER_KV);
    private static final int ARRAY_SIZE_INDEX = COLUMN_KV_COLUMNS.indexOf(ARRAY_SIZE_KV);
    private static final int VIEW_CONSTANT_INDEX = COLUMN_KV_COLUMNS.indexOf(VIEW_CONSTANT_KV);
    private static final int IS_VIEW_REFERENCED_INDEX = COLUMN_KV_COLUMNS.indexOf(IS_VIEW_REFERENCED_KV);
    private static final int COLUMN_DEF_INDEX = COLUMN_KV_COLUMNS.indexOf(COLUMN_DEF_KV);
    private static final int IS_ROW_TIMESTAMP_INDEX = COLUMN_KV_COLUMNS.indexOf(IS_ROW_TIMESTAMP_KV);

    private static final int LINK_TYPE_INDEX = 0;

    private static final KeyValue CLASS_NAME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, CLASS_NAME_BYTES);
    private static final KeyValue JAR_PATH_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, JAR_PATH_BYTES);
    private static final KeyValue RETURN_TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, RETURN_TYPE_BYTES);
    private static final KeyValue NUM_ARGS_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, NUM_ARGS_BYTES);
    private static final KeyValue TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TYPE_BYTES);
    private static final KeyValue IS_CONSTANT_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, IS_CONSTANT_BYTES);
    private static final KeyValue DEFAULT_VALUE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DEFAULT_VALUE_BYTES);
    private static final KeyValue MIN_VALUE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, MIN_VALUE_BYTES);
    private static final KeyValue MAX_VALUE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, MAX_VALUE_BYTES);
    private static final KeyValue IS_ARRAY_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, IS_ARRAY_BYTES);

    private static final List<KeyValue> FUNCTION_KV_COLUMNS = Arrays.<KeyValue>asList(
            EMPTY_KEYVALUE_KV,
            CLASS_NAME_KV,
            JAR_PATH_KV,
            RETURN_TYPE_KV,
            NUM_ARGS_KV
    );

    static {
        Collections.sort(FUNCTION_KV_COLUMNS, KeyValue.COMPARATOR);
    }

    private static final List<KeyValue> FUNCTION_ARG_KV_COLUMNS = Arrays.<KeyValue>asList(
            TYPE_KV,
            IS_ARRAY_KV,
            IS_CONSTANT_KV,
            DEFAULT_VALUE_KV,
            MIN_VALUE_KV,
            MAX_VALUE_KV
    );

    static {
        Collections.sort(FUNCTION_ARG_KV_COLUMNS, KeyValue.COMPARATOR);
    }


    //Map of all tables for further integrity check.
    static Map<String, PTableImpl> allTables = new HashMap<>();

    HBaseAdmin admin;
    HTable catalog;
    Configuration conf;

    public SystemCatalogCheck(HBaseAdmin admin) {
        this.admin = admin;
        conf = admin.getConfiguration();
    }


    /**
     * Entry point for system catalog integrity check
     * @param admin
     * @throws IOException
     * @throws SQLException
     */
    public static void check(HBaseAdmin admin) throws IOException, SQLException {
        SystemCatalogCheck sc = new SystemCatalogCheck(admin);
        sc.checkConsistency();
    }


    public boolean checkConsistency() throws IOException, SQLException {
        getTableList();
        ConsoleUI.infoMessage("\nStarting cross table integrity check \n");
        integrityCheck();
        ConsoleUI.infoMessage("\nSystem catalog consistency check is completed.");
        return true;
    }

    private void integrityCheck() throws IOException {
        List<PTable> indexTables = new ArrayList<>();
        List<PTable> userTables = new ArrayList<>();
        List<PTable> systemTables = new ArrayList<>();
        Set<String> tableNames = allTables.keySet();

        //Separate all tables into different lists
        for(String tableName : tableNames) {
            PTable table = allTables.get(tableName);
            switch (table.getType()) {
                case TABLE:
                    userTables.add(table);
                    break;
                case INDEX:
                    indexTables.add(table);
                    break;
                case SYSTEM:
                    systemTables.add(table);
                    break;
            }
        }
        // Iterate over user tables. If physical table doesn't exist, delete the record and all corresponding index records
        for(PTable table : userTables) {
            PName pname = table.getPhysicalName();
            try {
                HBaseUtils.checkTableEnabledAndOnline(pname.getBytes(), admin);
                //Validate indexes
                List<PTable> indexes = table.getIndexes();
                for (PTable index : indexes) {
                    try {
                        HBaseUtils.checkTableEnabledAndOnline(index.getPhysicalName().getBytes(), admin);
                    } catch (TableNotFoundException e ) {
                        ConsoleUI.infoMessage(" |-- Unable to find the physical table for index " + index.getTableName());
                        int answer = ConsoleUI.question("\nClean the corresponding record in the catalog and update user table ?\n", new String[]{
                                "Yes",
                                "No"
                        });
                        if (answer == 1) {
                            deleteTable(index);
                            deleteRecordForIndexFromTable(table,index);
                            ConsoleUI.infoMessage(" |-- Record for " + index.getName().getString() + " deleted from system catalog");
                        }
                    }
                }

            } catch (TableNotFoundException e) {
                ConsoleUI.infoMessage("Unable to find the physical table for " + table.getTableName());
                int answer = ConsoleUI.question("\nClean the corresponding record in the catalog?\n", new String[]{
                        "Yes",
                        "No"
                });
                if (answer == 1) {
                    deleteTable(table);
                    ConsoleUI.infoMessage("Record for " + table.getName().getString() + " deleted");
                    List<PTable> indexes = table.getIndexes();
                    for (PTable index : indexes) {
                        ConsoleUI.infoMessage(" |-- Cleaning record for corresponding index " + index.getName().getString());
                        deleteTable(index);
                    }
                }
            }
        }
    }

    private void deleteRecordForIndexFromTable(PTable table, PTable index) throws IOException {
        byte[] tablekey = SchemaUtil.getTableKey(table.getTenantId() == null ?
                ByteUtil.EMPTY_BYTE_ARRAY : table.getTenantId().getBytes(), table.getSchemaName().getBytes(), table.getTableName().getBytes());
        byte key[] = ByteUtil.concat(tablekey, QueryConstants.SEPARATOR_BYTE_ARRAY, QueryConstants.SEPARATOR_BYTE_ARRAY, index.getTableName().getBytes());
        Delete del = new Delete(key);
        catalog.delete(del);
    }

    /**
     * Perform the scan over catalog for Rows that contains BASE_COLUMN_COUNT qualifier. Gets the table name
     * and build Phoenix PTable instance that may be used for cross table integrity check
     * TODO: find a better way to identify entry rows for table records.
     * @throws IOException
     * @throws SQLException
     */
    public void getTableList() throws IOException, SQLException {
        Scan scan = new Scan();
        Filter f = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(BASE_COLUMN_COUNT_KV.getQualifier()));
        scan.setFilter(f);
        catalog = new HTable(conf, PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        ResultScanner scanner = catalog.getScanner(scan);

        while (true) {
            Result result = scanner.next();
            if (result == null || result.isEmpty()) {
                break;
            }
            List<Cell> results = result.listCells();
            Cell keyValue = results.get(0);
            byte[] keyBuffer = keyValue.getRowArray();
            int keyLength = keyValue.getRowLength();
            int keyOffset = keyValue.getRowOffset();
            PName tenantId = newPName(keyBuffer, keyOffset, keyLength);
            int tenantIdLength = (tenantId == null) ? 0 : tenantId.getBytes().length;
            if (tenantIdLength == 0) {
                tenantId = null;
            }
            PName schemaName = newPName(keyBuffer, keyOffset + tenantIdLength + 1, keyLength);
            int schemaNameLength = schemaName.getBytes().length;
            int tableNameLength = keyLength - schemaNameLength - 1 - tenantIdLength - 1;
            byte[] tableNameBytes = new byte[tableNameLength];
            System.arraycopy(keyBuffer, keyOffset + schemaNameLength + 1 + tenantIdLength + 1,
                    tableNameBytes, 0, tableNameLength);
            PName tableName = PNameFactory.newName(tableNameBytes);
            ConsoleUI.infoMessage("\nFound record for table " + SchemaUtil.getTableName(schemaName.getBytes(), tableName.getBytes()));
            byte[] key = SchemaUtil.getTableKey(tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId.getBytes(), schemaName.getBytes(), tableName.getBytes());
            PTable table = getTable(key);
        }
    }

    private void deleteTable(PTable table) throws IOException {
        byte[] key = SchemaUtil.getTableKey(table.getTenantId() == null?
                ByteUtil.EMPTY_BYTE_ARRAY : table.getTenantId().getBytes(), table.getSchemaName().getBytes(), table.getTableName().getBytes());
        deleteTableRecord(key);
    }


    private PTableImpl getTable(byte[] name) throws IOException, SQLException {

        Scan scan = MetaDataUtil.newTableRowsScan(name, MetaDataProtocol.MIN_TABLE_TIMESTAMP, HConstants.LATEST_TIMESTAMP);
        ResultScanner scanner = catalog.getScanner(scan);
        Result result = scanner.next();
        if(result == null || result.isEmpty()) {
            return null;
        }
        Cell[] tableKeyValues = new Cell[TABLE_KV_COLUMNS.size()];
        Cell[] colKeyValues = new Cell[COLUMN_KV_COLUMNS.size()];
        List<Cell> results = result.listCells();
        // Create PTable based on KeyValues from scan
        Cell keyValue = results.get(0);
        byte[] keyBuffer = keyValue.getRowArray();
        int keyLength = keyValue.getRowLength();
        int keyOffset = keyValue.getRowOffset();
        PName tenantId = newPName(keyBuffer, keyOffset, keyLength);
        int tenantIdLength = (tenantId == null) ? 0 : tenantId.getBytes().length;
        if (tenantIdLength == 0) {
            tenantId = null;
        }
        PName schemaName = newPName(keyBuffer, keyOffset + tenantIdLength + 1, keyLength);
        int schemaNameLength = schemaName.getBytes().length;
        int tableNameLength = keyLength - schemaNameLength - 1 - tenantIdLength - 1;
        byte[] tableNameBytes = new byte[tableNameLength];
        System.arraycopy(keyBuffer, keyOffset + schemaNameLength + 1 + tenantIdLength + 1,
                tableNameBytes, 0, tableNameLength);
        PName tableName = PNameFactory.newName(tableNameBytes);
        int offset = tenantIdLength + schemaNameLength + tableNameLength + 3;
        long timeStamp = keyValue.getTimestamp();

        int i = 0;
        int j = 0;
        while (i < results.size() && j < TABLE_KV_COLUMNS.size()) {
            Cell kv = results.get(i);
            Cell searchKv = TABLE_KV_COLUMNS.get(j);
            int cmp =
                    Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(),
                            kv.getQualifierLength(), searchKv.getQualifierArray(),
                            searchKv.getQualifierOffset(), searchKv.getQualifierLength());
            if (cmp == 0) {
                timeStamp = Math.max(timeStamp, kv.getTimestamp()); // Find max timestamp of table
                // header row
                tableKeyValues[j++] = kv;
                i++;
            } else if (cmp > 0) {
                timeStamp = Math.max(timeStamp, kv.getTimestamp());
                tableKeyValues[j++] = null;
            } else {
                ConsoleUI.failure(" |-- WARNING: Unexpected KV in system table header row : " + kv + ".");
                i++; // shouldn't happen - means unexpected KV in system table header row
            }
        }
        // TABLE_TYPE, TABLE_SEQ_NUM and COLUMN_COUNT are required.
        if (tableKeyValues[TABLE_TYPE_INDEX] == null || tableKeyValues[TABLE_SEQ_NUM_INDEX] == null
                || tableKeyValues[COLUMN_COUNT_INDEX] == null) {
            ConsoleUI.infoMessage(" |-- The record doesn't have required key values. That may prevent Phoenix running correctly");
            int answer = ConsoleUI.question("Choose the action", new String[] {
                    "Delete this record",
                    "Ignore and continue"
            });
            switch(answer) {
                case 1:
                    // Scan for all records related to this table and delete them.
                    deleteTableRecord(name);
                    return null;
                case 2:
                    break;
            }
        }

        Cell tableTypeKv = tableKeyValues[TABLE_TYPE_INDEX];
        PTableType tableType =
                PTableType
                        .fromSerializedValue(tableTypeKv.getValueArray()[tableTypeKv.getValueOffset()]);
        Cell tableSeqNumKv = tableKeyValues[TABLE_SEQ_NUM_INDEX];
        long tableSeqNum =
                PLong.INSTANCE.getCodec().decodeLong(tableSeqNumKv.getValueArray(),
                        tableSeqNumKv.getValueOffset(), SortOrder.getDefault());
        Cell columnCountKv = tableKeyValues[COLUMN_COUNT_INDEX];
        int columnCount =
                PInteger.INSTANCE.getCodec().decodeInt(columnCountKv.getValueArray(),
                        columnCountKv.getValueOffset(), SortOrder.getDefault());

        Cell pkNameKv = tableKeyValues[PK_NAME_INDEX];
        PName pkName =
                pkNameKv != null ? newPName(pkNameKv.getValueArray(), pkNameKv.getValueOffset(),
                        pkNameKv.getValueLength()) : null;
        Cell saltBucketNumKv = tableKeyValues[SALT_BUCKETS_INDEX];
        Integer saltBucketNum =
                saltBucketNumKv != null ? (Integer) PInteger.INSTANCE.getCodec().decodeInt(
                        saltBucketNumKv.getValueArray(), saltBucketNumKv.getValueOffset(), SortOrder.getDefault()) : null;
        if (saltBucketNum != null && saltBucketNum.intValue() == 0) {
            saltBucketNum = null; // Zero salt buckets means not salted
        }
        Cell dataTableNameKv = tableKeyValues[DATA_TABLE_NAME_INDEX];
        PName dataTableName =
                dataTableNameKv != null ? newPName(dataTableNameKv.getValueArray(),
                        dataTableNameKv.getValueOffset(), dataTableNameKv.getValueLength()) : null;
        Cell indexStateKv = tableKeyValues[INDEX_STATE_INDEX];
        PIndexState indexState =
                indexStateKv == null ? null : PIndexState.fromSerializedValue(indexStateKv
                        .getValueArray()[indexStateKv.getValueOffset()]);
        Cell immutableRowsKv = tableKeyValues[IMMUTABLE_ROWS_INDEX];
        boolean isImmutableRows =
                immutableRowsKv == null ? false : (Boolean) PBoolean.INSTANCE.toObject(
                        immutableRowsKv.getValueArray(), immutableRowsKv.getValueOffset(),
                        immutableRowsKv.getValueLength());
        Cell defaultFamilyNameKv = tableKeyValues[DEFAULT_COLUMN_FAMILY_INDEX];
        PName defaultFamilyName = defaultFamilyNameKv != null ? newPName(defaultFamilyNameKv.getValueArray(), defaultFamilyNameKv.getValueOffset(), defaultFamilyNameKv.getValueLength()) : null;
        Cell viewStatementKv = tableKeyValues[VIEW_STATEMENT_INDEX];
        String viewStatement = viewStatementKv != null ? (String) PVarchar.INSTANCE.toObject(viewStatementKv.getValueArray(), viewStatementKv.getValueOffset(),
                viewStatementKv.getValueLength()) : null;
        Cell disableWALKv = tableKeyValues[DISABLE_WAL_INDEX];
        boolean disableWAL = disableWALKv == null ? PTable.DEFAULT_DISABLE_WAL : Boolean.TRUE.equals(
                PBoolean.INSTANCE.toObject(disableWALKv.getValueArray(), disableWALKv.getValueOffset(), disableWALKv.getValueLength()));
        Cell multiTenantKv = tableKeyValues[MULTI_TENANT_INDEX];
        boolean multiTenant = multiTenantKv == null ? false : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(multiTenantKv.getValueArray(), multiTenantKv.getValueOffset(), multiTenantKv.getValueLength()));
        Cell storeNullsKv = tableKeyValues[STORE_NULLS_INDEX];
        boolean storeNulls = storeNullsKv == null ? false : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(storeNullsKv.getValueArray(), storeNullsKv.getValueOffset(), storeNullsKv.getValueLength()));
        Cell transactionalKv = tableKeyValues[TRANSACTIONAL_INDEX];
        boolean transactional = transactionalKv == null ? false : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(transactionalKv.getValueArray(), transactionalKv.getValueOffset(), transactionalKv.getValueLength()));
        Cell viewTypeKv = tableKeyValues[VIEW_TYPE_INDEX];
        PTable.ViewType viewType = viewTypeKv == null ? null : PTable.ViewType.fromSerializedValue(viewTypeKv.getValueArray()[viewTypeKv.getValueOffset()]);
        Cell viewIndexIdKv = tableKeyValues[VIEW_INDEX_ID_INDEX];
        Short viewIndexId = viewIndexIdKv == null ? null : (Short) MetaDataUtil.getViewIndexIdDataType().getCodec().decodeShort(viewIndexIdKv.getValueArray(), viewIndexIdKv.getValueOffset(), SortOrder.getDefault());
        Cell indexTypeKv = tableKeyValues[INDEX_TYPE_INDEX];
        PTable.IndexType indexType = indexTypeKv == null ? null : PTable.IndexType.fromSerializedValue(indexTypeKv.getValueArray()[indexTypeKv.getValueOffset()]);
        Cell baseColumnCountKv = tableKeyValues[BASE_COLUMN_COUNT_INDEX];
        int baseColumnCount = baseColumnCountKv == null ? 0 : PInteger.INSTANCE.getCodec().decodeInt(baseColumnCountKv.getValueArray(),
                baseColumnCountKv.getValueOffset(), SortOrder.getDefault());
        Cell rowKeyOrderOptimizableKv = tableKeyValues[ROW_KEY_ORDER_OPTIMIZABLE_INDEX];
        boolean rowKeyOrderOptimizable = rowKeyOrderOptimizableKv == null ? false : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(rowKeyOrderOptimizableKv.getValueArray(), rowKeyOrderOptimizableKv.getValueOffset(), rowKeyOrderOptimizableKv.getValueLength()));
        Cell updateCacheFrequencyKv = tableKeyValues[UPDATE_CACHE_FREQUENCY_INDEX];
        long updateCacheFrequency = updateCacheFrequencyKv == null ? 0 :
                PLong.INSTANCE.getCodec().decodeLong(updateCacheFrequencyKv.getValueArray(),
                        updateCacheFrequencyKv.getValueOffset(), SortOrder.getDefault());
        Cell indexDisableTimestampKv = tableKeyValues[INDEX_DISABLE_TIMESTAMP];
        long indexDisableTimestamp = indexDisableTimestampKv == null ? 0L : PLong.INSTANCE.getCodec().decodeLong(indexDisableTimestampKv.getValueArray(),
                indexDisableTimestampKv.getValueOffset(), SortOrder.getDefault());
        Cell isNamespaceMappedKv = tableKeyValues[IS_NAMESPACE_MAPPED_INDEX];
        boolean isNamespaceMapped = isNamespaceMappedKv == null ? false
                : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(isNamespaceMappedKv.getValueArray(),
                isNamespaceMappedKv.getValueOffset(), isNamespaceMappedKv.getValueLength()));
        // 4.9 KVs.
//        Cell autoPartitionSeqKv = tableKeyValues[AUTO_PARTITION_SEQ_INDEX];
//        String autoPartitionSeq = autoPartitionSeqKv != null ? (String) PVarchar.INSTANCE.toObject(autoPartitionSeqKv.getValueArray(), autoPartitionSeqKv.getValueOffset(),
//                autoPartitionSeqKv.getValueLength()) : null;
//        Cell isAppendOnlySchemaKv = tableKeyValues[APPEND_ONLY_SCHEMA_INDEX];
//        boolean isAppendOnlySchema = isAppendOnlySchemaKv == null ? false
//                : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(isAppendOnlySchemaKv.getValueArray(),
//                isAppendOnlySchemaKv.getValueOffset(), isAppendOnlySchemaKv.getValueLength()));
        List<PColumn> columns = Lists.newArrayListWithExpectedSize(columnCount);
        List<PTable> indexes = Lists.newArrayList();
        List<PName> physicalTables = Lists.newArrayList();
        PName parentTableName = tableType == INDEX ? dataTableName : null;
        PName parentSchemaName = tableType == INDEX ? schemaName : null;
        ConsoleUI.infoMessage(" |-- Checking table internal structure...");
        while (true) {
            Result columnResult = scanner.next();
            if (columnResult == null) {
                break;
            }
            results = columnResult.listCells();
            if (results.isEmpty()) {
                break;
            }
            Cell colKv = results.get(LINK_TYPE_INDEX);
            int colKeyLength = colKv.getRowLength();
            PName colName = newPName(colKv.getRowArray(), colKv.getRowOffset() + offset, colKeyLength - offset);
            int colKeyOffset = offset + colName.getBytes().length + 1;
            PName famName = newPName(colKv.getRowArray(), colKv.getRowOffset() + colKeyOffset, colKeyLength - colKeyOffset);
            if (colName.getString().isEmpty() && famName != null) {
                PTable.LinkType linkType = PTable.LinkType.fromSerializedValue(colKv.getValueArray()[colKv.getValueOffset()]);
                if (linkType == PTable.LinkType.INDEX_TABLE) {
                    PTable index = addIndexToTable(tenantId, schemaName, famName, tableName, HConstants.LATEST_TIMESTAMP, indexes);
                    if(index == null) {
                        // Incorrect record for the index. We need to clean up it.
                        int answer = ConsoleUI.question("Unable to obtain index information. Clean it?", new String[]{
                                "Yes",
                                "No"
                        });
                        if(answer == 1) {
                            catalog.delete(new Delete(columnResult.getRow()));
                            ConsoleUI.infoMessage("Done");
                        }
                    }
                } else if (linkType == PTable.LinkType.PHYSICAL_TABLE) {
                    physicalTables.add(famName);
                } else if (linkType == PTable.LinkType.PARENT_TABLE) {
                    parentTableName = PNameFactory.newName(SchemaUtil.getTableNameFromFullName(famName.getBytes()));
                    parentSchemaName = PNameFactory.newName(SchemaUtil.getSchemaNameFromFullName(famName.getBytes()));
                }
            } else {
                addColumnToTable(results, colName, famName, colKeyValues, columns, saltBucketNum != null);
            }
        }

        boolean addToList = true;

        if (columnCount != columns.size()) {
            int answer = ConsoleUI.question("The number of columns records doesn't match the table meta information. " +
                    "Update meta information?", new String[]{
                    "Yes",
                    "No"
            });
            if (answer == 1) {
                // Update Meta row.
                Put put = new Put(result.getRow());
                byte[] ptr = new byte[PInteger.INSTANCE.getByteSize()];
                PInteger.INSTANCE.getCodec().encodeInt(columns.size(), ptr, 0);

                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.COLUMN_COUNT_BYTES, ptr);
                catalog.put(put);
                columnCount = columns.size();
            } else {
                ConsoleUI.infoMessage("Consider to fix it in the future because this type of problem may lead to unpredictable exceptions when Phoenix is working");
                return null;
            }

        }
        int deletedColumns = 0;
        for (Iterator<PColumn> iterator = columns.listIterator(); iterator.hasNext(); ) {
            PColumn col = iterator.next();
            if ((saltBucketNum != null ? 2 : 1) + col.getPosition() > columnCount) {
                int answer = ConsoleUI.question("Column " + col.getName() + " has incorrect position " +
                        (col.getPosition() + 1) + ". Please choose the action: ", new String[]{
                        "Delete column record",
                        "Ignore"
                        });

                switch (answer) {
                    case 1:
                        byte[] key = getColumnKey(tenantId == null ? null : tenantId.getString(), schemaName.getString(), tableName.getString(), col.getName().getString(), col.getFamilyName().getString());
                        Delete del = new Delete(key);
                        catalog.delete(del);
                        iterator.remove();
                        deletedColumns++;
                        break;
                    case 2:
                        ConsoleUI.infoMessage("Ignoring may cause problems with accessing the table further. \n " +
                                "Consider to run Repair tool again to get in fixed.");
                        addToList = false;
                        break;
                }
            }
        }
        //Update number of columns in meta data.
        if (deletedColumns > 0) {
            columnCount = columns.size();
            Put put = new Put(result.getRow());
            byte[] ptr = new byte[PInteger.INSTANCE.getByteSize()];
            PInteger.INSTANCE.getCodec().encodeInt(columnCount, ptr, 0);
            put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.COLUMN_COUNT_BYTES, ptr);
            catalog.put(put);

        }

        if (addToList) {
            PTableImpl x = PTableImpl.makePTable(tenantId, schemaName, tableName, tableType, indexState, timeStamp, tableSeqNum,
                    pkName, saltBucketNum, columns, parentSchemaName, parentTableName, indexes, isImmutableRows, physicalTables, defaultFamilyName,
                    viewStatement, disableWAL, multiTenant, storeNulls, viewType, viewIndexId, indexType,
                    rowKeyOrderOptimizable, transactional, updateCacheFrequency, baseColumnCount,
                    indexDisableTimestamp, isNamespaceMapped); // 4.9:, autoPartitionSeq, isAppendOnlySchema);
            allTables.put(x.toString(), x);
            return x;

        }
        return null;
    }

    /**
     * Scan for the specific table key and delete all rows that are related (like column/index records)
     * @param name
     * @throws IOException
     */
    private void deleteTableRecord(byte[] name) throws IOException {
        Scan scanToDelete = MetaDataUtil.newTableRowsScan(name, MetaDataProtocol.MIN_TABLE_TIMESTAMP, HConstants.LATEST_TIMESTAMP);
        ResultScanner sc = catalog.getScanner(scanToDelete);
        while(true) {
            Result res = sc.next();
            if (res == null || res.isEmpty()) {
                break;
            }
            Delete del = new Delete(res.getRow());
            catalog.delete(del);
        }
    }


    private  void addColumnToTable(List<Cell> results, PName colName, PName famName,
                                         Cell[] colKeyValues, List<PColumn> columns, boolean isSalted) {
        int i = 0;
        int j = 0;
        while (i < results.size() && j < COLUMN_KV_COLUMNS.size()) {
            Cell kv = results.get(i);
            Cell searchKv = COLUMN_KV_COLUMNS.get(j);
            int cmp =
                    Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(),
                            kv.getQualifierLength(), searchKv.getQualifierArray(),
                            searchKv.getQualifierOffset(), searchKv.getQualifierLength());
            if (cmp == 0) {
                colKeyValues[j++] = kv;
                i++;
            } else if (cmp > 0) {
                colKeyValues[j++] = null;
            } else {
                i++; // shouldn't happen - means unexpected KV in system table column row
            }
        }
        if (colKeyValues[DATA_TYPE_INDEX] == null || colKeyValues[NULLABLE_INDEX] == null
                || colKeyValues[ORDINAL_POSITION_INDEX] == null) {
            throw new IllegalStateException("Didn't find all required key values in '"
                    + colName.getString() + "' column metadata row");
        }

        Cell columnSizeKv = colKeyValues[COLUMN_SIZE_INDEX];
        Integer maxLength =
                columnSizeKv == null ? null : PInteger.INSTANCE.getCodec().decodeInt(
                        columnSizeKv.getValueArray(), columnSizeKv.getValueOffset(), SortOrder.getDefault());
        Cell decimalDigitKv = colKeyValues[DECIMAL_DIGITS_INDEX];
        Integer scale =
                decimalDigitKv == null ? null : PInteger.INSTANCE.getCodec().decodeInt(
                        decimalDigitKv.getValueArray(), decimalDigitKv.getValueOffset(), SortOrder.getDefault());
        Cell ordinalPositionKv = colKeyValues[ORDINAL_POSITION_INDEX];
        int position =
                PInteger.INSTANCE.getCodec().decodeInt(ordinalPositionKv.getValueArray(),
                        ordinalPositionKv.getValueOffset(), SortOrder.getDefault()) + (isSalted ? 1 : 0);
        Cell nullableKv = colKeyValues[NULLABLE_INDEX];
        boolean isNullable =
                PInteger.INSTANCE.getCodec().decodeInt(nullableKv.getValueArray(),
                        nullableKv.getValueOffset(), SortOrder.getDefault()) != ResultSetMetaData.columnNoNulls;
        Cell dataTypeKv = colKeyValues[DATA_TYPE_INDEX];
        PDataType dataType =
                PDataType.fromTypeId(PInteger.INSTANCE.getCodec().decodeInt(
                        dataTypeKv.getValueArray(), dataTypeKv.getValueOffset(), SortOrder.getDefault()));
        if (maxLength == null && dataType == PBinary.INSTANCE) dataType = PVarbinary.INSTANCE;   // For
        // backward
        // compatibility.
        Cell sortOrderKv = colKeyValues[SORT_ORDER_INDEX];
        SortOrder sortOrder =
                sortOrderKv == null ? SortOrder.getDefault() : SortOrder.fromSystemValue(PInteger.INSTANCE
                        .getCodec().decodeInt(sortOrderKv.getValueArray(),
                                sortOrderKv.getValueOffset(), SortOrder.getDefault()));

        Cell arraySizeKv = colKeyValues[ARRAY_SIZE_INDEX];
        Integer arraySize = arraySizeKv == null ? null :
                PInteger.INSTANCE.getCodec().decodeInt(arraySizeKv.getValueArray(), arraySizeKv.getValueOffset(), SortOrder.getDefault());

        Cell viewConstantKv = colKeyValues[VIEW_CONSTANT_INDEX];
        byte[] viewConstant = viewConstantKv == null ? null : viewConstantKv.getValue();
        Cell isViewReferencedKv = colKeyValues[IS_VIEW_REFERENCED_INDEX];
        boolean isViewReferenced = isViewReferencedKv != null && Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(isViewReferencedKv.getValueArray(), isViewReferencedKv.getValueOffset(), isViewReferencedKv.getValueLength()));
        Cell columnDefKv = colKeyValues[COLUMN_DEF_INDEX];
        String expressionStr = columnDefKv==null ? null : (String)PVarchar.INSTANCE.toObject(columnDefKv.getValueArray(), columnDefKv.getValueOffset(), columnDefKv.getValueLength());
        Cell isRowTimestampKV = colKeyValues[IS_ROW_TIMESTAMP_INDEX];
        boolean isRowTimestamp =
                isRowTimestampKV == null ? false : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(
                        isRowTimestampKV.getValueArray(), isRowTimestampKV.getValueOffset(),
                        isRowTimestampKV.getValueLength()));

        PColumn column = new PColumnImpl(colName, famName, dataType, maxLength, scale, isNullable, position-1, sortOrder, arraySize, viewConstant, isViewReferenced, expressionStr, isRowTimestamp, false);
        columns.add(column);

    }
    private PTable addIndexToTable(PName tenantId, PName schemaName, PName indexName, PName tableName, long clientTimeStamp, List<PTable> indexes) throws IOException, SQLException {
        byte[] key = SchemaUtil.getTableKey(tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId.getBytes(), schemaName.getBytes(), indexName.getBytes());
        PTable indexTable = getTable(key);
        if (indexTable != null) {
            indexes.add(indexTable);
        }
        return indexTable;
    }

    private  PName newPName(byte[] keyBuffer, int keyOffset, int keyLength) {
        if (keyLength <= 0) {
            return null;
        }
        int length = getVarCharLength(keyBuffer, keyOffset, keyLength);
        return PNameFactory.newName(keyBuffer, keyOffset, length);
    }

    public  byte[] getColumnKey(String tenantId, String schemaName, String tableName, String columnName, String familyName) {
        if (familyName == null) {
            return ByteUtil.concat(tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(tenantId),
                    QueryConstants.SEPARATOR_BYTE_ARRAY, schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName),
                    QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(tableName),
                    QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(columnName));
        }
        return ByteUtil.concat(tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(tenantId),
                QueryConstants.SEPARATOR_BYTE_ARRAY, schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName),
                QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(tableName),
                QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(columnName),
                QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(familyName));
    }


    /**
     * We need it for testing purposes.
     * @param args
     * @throws IOException
     * @throws SQLException
     */
    public static void main(String[] args) throws IOException, SQLException {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        SystemCatalogCheck sc = new SystemCatalogCheck(admin);
        sc.check(admin);

    }


}
