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

package org.apache.phoenix.util.repairtool.utils;

import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.util.repairtool.ConsoleUI;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;

public class HBaseUtils {

    /**
     * Check table status. If it's disabled, offer to enable it. If it doesn't exist, return false.
     *
     * @param tableName
     * @param admin
     * @return
     * @throws IOException
     */
    public static boolean checkTableEnabledAndOnline(byte[] tableName, HBaseAdmin admin) throws IOException {
        if (!admin.isTableEnabled(tableName)) {
            int answer = ConsoleUI.question(SchemaUtil.getTableNameFromFullName(tableName) + " is disabled. Do you want to enable it?", new String[]{"Yes", "No"});
            if (answer == 1) {
                try {
                    admin.enableTable(tableName);
                    if (!admin.isTableEnabled(tableName)) {
                        throw new RuntimeException("Unable to enable " + SchemaUtil.getTableNameFromFullName(tableName) + ". Consider to run hbck tool");
                    }
                } catch (TableNotDisabledException e) {
                    ConsoleUI.infoMessage("Table " + SchemaUtil.getTableNameFromFullName(tableName) + " is already enabled... ");
                }
            } else {
                return false;
            }
        }
        return true;
    }
}
