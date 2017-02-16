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

import java.util.Scanner;

public class ConsoleUI {

    public static int question (String intro, String[] questions) {
        System.out.println(intro);
        for(int i = 0; i <questions.length;i++) {
            System.out.println((i+1) + ". " +questions[i]);
        }
        int result = -1;
        while(result <= 0 || result >questions.length) {
            System.out.print("Your choice:");
            Scanner kbd = new Scanner(System.in);
            result = kbd.nextInt();
        }
        return result;
    }

    public static void failure(String message) {
        System.out.println(message);
    }

    public static void infoMessage(String message) {
        System.out.println(message);
    }

}
