/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.audit;

public class AuditLogOptions
{
    public boolean enabled = false;
    public String logger = FileAuditLogger.class.getName();
    public String included_keyspaces = "";
    public String excluded_keyspaces = "";
    public String included_categories = "";
    public String excluded_categories = "";
    public String included_users = "";
    public String excluded_users= "";

    /**
     * AuditLogs directory can be configured using `cassandra.logdir.audit` or default is set to `cassandra.logdir` + /audit/
     */
    public String audit_logs_dir = System.getProperty("cassandra.logdir.audit",
                                                      System.getProperty("cassandra.logdir",".")+"/audit/");
    /**
     * Blocking Whether the AuditLog should block if the FQL falls behind or should drop audit log records. Default is set to true so that AuditLog records wont be lost
     */
    public boolean block = true;

    /**
     * max_queue_weight: Maximum weight of in memory queue for records waiting to be written to the audit log file before blocking or dropping the log records. For advanced configurations
     */
    public int max_queue_weight = 256 * 1024 * 1024;
    /**
     * max_log_size: Maximum size of the rolled files to retain on disk before deleting the oldest file. For advanced configurations
     */
    public long max_log_size = 16L * 1024L * 1024L * 1024L;
    /**
     * roll_cycle: How often to roll Audit log segments so they can potentially be reclaimed. Available options are:
     * MINUTELY, HOURLY, DAILY, LARGE_DAILY, XLARGE_DAILY, HUGE_DAILY. For more options, refer: net.openhft.chronicle.queue.RollCycles
     */
    public String roll_cycle = "HOURLY";
}
