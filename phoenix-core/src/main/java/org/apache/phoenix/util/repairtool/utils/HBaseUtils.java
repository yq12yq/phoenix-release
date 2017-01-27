package org.apache.phoenix.util.repairtool.utils;

import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.util.repairtool.ConsoleUI;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;

/**
 * Created by ssoldatov on 1/25/17.
 */
public class HBaseUtils {

    public static boolean checkTableEnabledAndOnline(byte[] tableName, HBaseAdmin admin) throws IOException {
        try {
            if (!admin.isTableEnabled(tableName)) {
                int answer = ConsoleUI.question(SchemaUtil.getTableNameFromFullName(tableName) + " is disabled. Do you want to enable it?", new String[]{"Yes", "No"});
                if (answer == 1) {
                    try {
                        admin.enableTable(tableName);
                        if (!admin.isTableEnabled(tableName)) {
                            throw new RuntimeException("Unable to enable " + SchemaUtil.getTableNameFromFullName(tableName) + ". Consider to run hbck to repair HBase meta");
                        }
                    } catch (TableNotDisabledException e) {
                        ConsoleUI.infoMessage("Table " + SchemaUtil.getTableNameFromFullName(tableName) + " is already enabled... ");
                    }
                } else {
                    ConsoleUI.failure("Unable to enable table " + SchemaUtil.getTableNameFromFullName(tableName));
                    System.exit(-1);
                }
            }
            return true;
        } catch (TableNotFoundException e) {
            return false;
        }
    }
}
