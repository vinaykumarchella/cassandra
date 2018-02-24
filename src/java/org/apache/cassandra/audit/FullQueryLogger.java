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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

import org.apache.cassandra.cql3.QueryOptions;


/**
 * A logger that is NOOP implementation in pre 4.0 branches
 */
public class FullQueryLogger implements IAuditLogger
{
    @Override
    public boolean enabled()
    {
        return false;
    }

    @Override
    public void log(AuditLogEntry auditLogEntry)
    {

    }

    @Override
    public void stop()
    {

    }

    @Override
    public Path path()
    {
        return null;
    }

    public void configure(Path path, String rollCycle, boolean blocking, int maxQueueWeight, long maxLogSize)
    {
    }

    public void reset(String fullQueryLogPath)
    {
    }

    public void logBatch(String batchTypeName, List<String> queryStrings, List<List<ByteBuffer>> values, QueryOptions options, long queryStartTimeMillis)
    {
    }
}
