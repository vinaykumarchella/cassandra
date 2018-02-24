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

import java.nio.file.Path;

public interface IAuditLogger
{
    boolean enabled();

    /**
     * Logs AuditLogEntry. This method might be called after {@link #stop()},
     * hence implementations need to handle the race condition.
     */
    void log(AuditLogEntry auditLogEntry);

    /**
     * Stop and cleanup any resources of IAuditLogger implementations. Please note that
     * {@link #log(AuditLogEntry)} might be called after being stopped.
     */
    void stop();

    /**
     * @return the path to the logging files/directory if the implemenation writes out to the local filesystem,
     * or null if the implementation doesn't log locally.
     */
    Path path();
}
