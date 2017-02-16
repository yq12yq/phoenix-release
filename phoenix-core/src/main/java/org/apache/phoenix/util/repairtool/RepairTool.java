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

package org.apache.phoenix.util.repairtool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.util.repairtool.modules.SystemCatalogCheck;
import org.apache.phoenix.util.repairtool.utils.HBaseUtils;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;


/**
 * Repair tool does:
 * 1. Consistency check for system tables to fix failures like NullPointer or TableNotFound exceptions
 * 2. Check that indexes are maintained and match the corresponding tables.
 * 3. (TODO) Check Statistic table for matching between records and existing regions.
 *
 * For catalog check HBase client is used because there is a chance that even initialization of Phoenix driver may cause failure
 * The rest may use both HBase and Phoenix client.
 *
 * NOTE: This is not hbck or fsck tool. When RepairTool is using, the healthy HDFS and HBase are expected and the
 * corresponding tools should be run prior RepairTool
 */



public class RepairTool {


    private HBaseAdmin admin;
    private PhoenixConnection conn;
    private String jdbcConn;
    private Configuration conf;


    public RepairTool(String conn) {
        this.jdbcConn = conn;
    }


    /**
     * Entry point to RepairTool.
     * @param conn
     * @throws IOException
     * @throws SQLException
     */
    public static void checkAndRepair(String conn) throws IOException, SQLException {
        RepairTool repairTool = new RepairTool(conn);
        repairTool.execute();

    }

    /**
     * Execute all checks one by one
     * @throws IOException
     * @throws SQLException
     */
    private void execute() throws IOException, SQLException {
        init();
        checkAndSetMaintenanceMode();
        checkSystemTablesEnabled();
        checkConsistancySystemCatalog();

        //Now we expect that system catalog is in working state, so we may try to create jdbc connection
        conn = DriverManager.getConnection(jdbcConn, new Properties()).unwrap(PhoenixConnection.class);
        complete();
    }

    /**
     * Setting up all required variables such as HBase Admin and check for previously stored snapshots.
     */
    private void init()  {
        try {
            conf = HBaseConfiguration.create();
            admin = new HBaseAdmin(conf);
            Pattern snapshotPattern = Pattern.compile(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME + ".*");
            List<HBaseProtos.SnapshotDescription> snapshots = admin.listSnapshots(snapshotPattern);
            if(snapshots.size() > 0) {
                // If we found previously stored snapshot(s), we offer : restore | delete all | continue without deleting | Exit
                List<String> snapshotNames = new ArrayList<String>(snapshots.size());
                ConsoleUI.infoMessage("\nPreviously stored snapshots has been found : \n");
                for(HBaseProtos.SnapshotDescription snapshot : snapshots) {
                    ConsoleUI.infoMessage(snapshot.getName());
                    snapshotNames.add(snapshot.getName());
                }
                int answer = ConsoleUI.question("\nPlease choose one of the actions: ", new String[]  {
                        "Restore from one of the previous snapshot and exit",
                        "Delete all snapshots and continue",
                        "Continue without deleting snapshots",
                        "Exit without action"
                });
                switch(answer) {
                    case 1:
                        answer = ConsoleUI.question("\nChoose the snapshot to restore: \n", snapshotNames.toArray(new String[snapshotNames.size()]));
                        admin.disableTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
                        admin.restoreSnapshot(snapshotNames.get(answer -1));
                        admin.enableTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
                        ConsoleUI.infoMessage("\nCatalog restore from snapshot successfully completed.");
                    case 4:
                        ConsoleUI.infoMessage("Exiting Repair Tool");
                        System.exit(0);
                        break;
                    case 2:
                        admin.deleteSnapshots(snapshotPattern);
                        break;
                    case 3:
                        break;
                }
            }
        } catch (Exception e) {
            ConsoleUI.failure("Failure during initialization. Check the HBase configuration");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * TODO: We need to disable everything on the Phoenix server side to avoid collisions during table manipulations.
     */
    private void checkAndSetMaintenanceMode() {
    }


    /**
     * Check that all system tables exist and enabled.
     * @throws IOException
     * @throws SQLException
     */
    private void checkSystemTablesEnabled() throws IOException, SQLException {
        if (!HBaseUtils.checkTableEnabledAndOnline(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, admin)) {
            //Catalog was not found. Instantiate Phoenix connection to perform table creation
            int answer = ConsoleUI.question("SYSTEM.CATALOG doesn't exist. Do you want to crate it?", new String[] {"Yes", "No"});
            if(answer == 1) {
                ConsoleUI.infoMessage("Trying instantiate Phoenix JDBC connection to create system tables");

                // The simple way to create all system tables. TODO: should we try to create it manually using HBase API?
                DriverManager.getConnection("jdbc:phoenix:localhost");
                if (!HBaseUtils.checkTableEnabledAndOnline(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, admin)) {
                    ConsoleUI.failure("Unable to initialize SYSTEM.CATALOG... Exiting");
                    System.exit(-1);
                }
            } else {
                ConsoleUI.failure("Unable to proceed without system catalog");
                System.exit(-1);
            }
        }
        HBaseUtils.checkTableEnabledAndOnline(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES, admin);
        HBaseUtils.checkTableEnabledAndOnline(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES, admin);

    }

    /**
     * Integrity check for the System catalog. It has risky operations, so it's important to keep snapshot
     * @throws IOException
     * @throws SQLException
     */

    private void checkConsistancySystemCatalog() throws IOException, SQLException {
        createCatalogSnapshot();
        SystemCatalogCheck.check(admin);
    }

    /**
     * Creating a snapshot for catalog
     * @throws IOException
     */
    private void createCatalogSnapshot() throws IOException {
        String snapshotName = PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME + "_" + new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        ConsoleUI.infoMessage("\nCreating catalog snapshot: " + snapshotName + "\n");
        admin.disableTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        admin.snapshot(snapshotName, PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
        admin.enableTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        ConsoleUI.infoMessage("Snapshot creationg completed");
    }


    private void complete() throws IOException, SQLException {
        conn.getQueryServices().clearCache();
    }
}
