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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;

public class AuditLogEntry
{
    private static final InetSocketAddress DEFAULT_SOURCE = new InetSocketAddress("0.0.0.0", 0);

    private final InetAddressAndPort host = FBUtilities.getBroadcastAddressAndPort();
    private final String source, user;
    private final int srcPort;
    private long timestamp;
    private AuditLogEntryType type;
    private UUID batch;
    private String keyspace, scope, operation = "";

    public AuditLogEntry(String source, int srcPort, String user)
    {
        this.source = source;
        this.user = user;
        this.srcPort = srcPort;
        timestamp = System.currentTimeMillis();
    }

    public AuditLogEntry(AuditLogEntryType type, String user, String source, int srcPort)
    {
        this(source, srcPort, user);
        this.type = type;
    }

    public AuditLogEntry(AuditLogEntry auditLogEntry)
    {
        this(auditLogEntry.type, auditLogEntry.user, auditLogEntry.source, auditLogEntry.srcPort);
        timestamp = auditLogEntry.timestamp;
        keyspace = auditLogEntry.keyspace;
        scope = auditLogEntry.scope;
        batch = auditLogEntry.batch;
        operation = auditLogEntry.operation;
    }

    public static AuditLogEntry getAuditEntry(ClientState clientState)
    {
        InetSocketAddress sourceSockAddr = getSource(clientState);

        if (clientState != null && clientState.getUser() != null)
        {
            return new AuditLogEntry(sourceSockAddr.getAddress().toString(), sourceSockAddr.getPort(), clientState.getUser().getName());
        }
        return new AuditLogEntry(sourceSockAddr.getAddress().toString(), sourceSockAddr.getPort(), AuthenticatedUser.SYSTEM_USER.getName());
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder(64);
        builder.append("user:").append(user)
               .append("|host:").append(host)
               .append("|source:").append(source);
        if (srcPort > 0)
        {
            builder.append("|port:").append(srcPort);
        }

        builder.append("|timestamp:").append(timestamp)
               .append("|type:").append(type)
               .append("|category:").append(type.getCategory());

        if (batch != null)
        {
            builder.append("|batch:").append(batch);
        }
        if (StringUtils.isNotBlank(keyspace))
        {
            builder.append("|ks:").append(keyspace);
        }
        if (StringUtils.isNotBlank(scope))
        {
            builder.append("|scope:").append(scope);
        }
        if (StringUtils.isNotBlank(operation))
        {
            builder.append("|operation:").append(operation);
        }
        return builder.toString();
    }

    public String getUser()
    {
        return this.user;
    }

    public AuditLogEntry setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
        return this;
    }

    public AuditLogEntry setBatch(UUID batch)
    {
        this.batch = batch;
        return this;
    }

    public String getOperation()
    {
        return operation;
    }

    public AuditLogEntry setOperation(String operation)
    {
        this.operation = operation;
        return this;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public AuditLogEntry setKeyspace(String keyspace)
    {
        this.keyspace = keyspace;
        return this;
    }

    public String getScope()
    {
        return scope;
    }

    public AuditLogEntry setScope(String scope)
    {
        this.scope = scope;
        return this;
    }

    public AuditLogEntryType getType()
    {
        return type;
    }

    public AuditLogEntry setType(AuditLogEntryType type)
    {
        this.type = type;
        return this;
    }

    public void appendToOperation(String str)
    {
        if (StringUtils.isNotBlank(str))
        {
            this.operation = this.operation.concat("; ").concat(str);
        }
    }

    private static InetSocketAddress getSource(ClientState state)
    {
        if (state != null && state.getRemoteAddress() != null)
        {
            return state.getRemoteAddress();
        }
        return DEFAULT_SOURCE;
    }

    public static AuditLogEntry getLogEntry(CQLStatement statement, String queryString, QueryState queryState, AuditLogEntryType type)
    {
        AuditLogEntry entry = AuditLogEntry.getAuditEntry(queryState.getClientState());

        entry.setKeyspace(getKeyspace(statement, queryState))
             .setScope(getColumnFamily(statement))
             .setOperation(queryString)
             .setType(type);

        return entry;
    }

    /**
     * Gets the AuditLogEntry entry as per the params given. Ensure that type is set by the caller.
     */
    public static AuditLogEntry getLogEntry(String operation, QueryState queryState, AuditLogEntryType type)
    {
        AuditLogEntry entry = AuditLogEntry.getAuditEntry(queryState.getClientState());
        entry.setKeyspace(queryState.getClientState().getRawKeyspace())
             .setOperation(operation)
             .setType(type);

        return entry;
    }

    /**
     * Native protocol/ CQL helper methods for Audit Logging
     */
    public static AuditLogEntry getLogEntry(CQLStatement statement, String queryString, QueryState queryState)
    {
        return getLogEntry(statement, queryString, queryState, statement.getAuditLogContext().auditLogEntryType);
    }

    public static AuditLogEntry getLogEntry(CQLStatement statement, String queryString, QueryState queryState, QueryOptions queryOptions)
    {
        return getLogEntry(statement, queryString, queryState);
    }

    /**
     * Gets the AuditLogEntry entry as per the params given. Ensure that type is set by the caller.
     */
    public static AuditLogEntry getLogEntry(String queryString, QueryState queryState, QueryOptions queryOptions)
    {
        AuditLogEntry entry = AuditLogEntry.getAuditEntry(queryState.getClientState());
        entry.setKeyspace(queryState.getClientState().getRawKeyspace()).setOperation(queryString);
        return entry;
    }

    /**
     * Gets the AuditLogEntry entry as per the params given. Ensure that type is set by the caller.
     */
    public static AuditLogEntry getLogEntry(String queryString, QueryState queryState, QueryOptions queryOptions, UUID batchId)
    {
        AuditLogEntry entry = AuditLogEntry.getAuditEntry(queryState.getClientState());

        entry.setKeyspace(queryState.getClientState().getRawKeyspace())
             .setBatch(batchId)
             .setType(AuditLogEntryType.BATCH)
             .setOperation(queryString);
        return entry;
    }

    private static AuditLogEntry getLogEntry(CQLStatement statement, String rawCQLStatement, QueryState queryState, QueryOptions queryOptions, UUID batchId)
    {
        return getLogEntry(statement, rawCQLStatement, queryState, queryOptions).setBatch(batchId);
    }

    public static List<AuditLogEntry> getLogEntriesForBatch(List<Object> queryOrIdList, List<ParsedStatement.Prepared> prepared, QueryState state, QueryOptions options)
    {
        List<AuditLogEntry> auditLogEntries = Lists.newArrayList();
        UUID batchId = UUID.randomUUID();
        String queryString = String.format("BatchId:[%s] - BATCH of [%d] statements", batchId, queryOrIdList.size());
        auditLogEntries.add(getLogEntry(queryString, state, options, batchId));

        for (int i = 0; i < queryOrIdList.size(); i++)
        {
            auditLogEntries.add(getLogEntry(prepared.get(i).statement, prepared.get(i).rawCQLStatement, state, options, batchId));
        }

        return auditLogEntries;
    }

    private static String getKeyspace(CQLStatement stmt, QueryState queryState)
    {
        return stmt.getAuditLogContext().keyspace != null ? stmt.getAuditLogContext().keyspace : queryState.getClientState().getRawKeyspace();
    }

    private static String getColumnFamily(CQLStatement stmt)
    {
        return stmt.getAuditLogContext().scope;
    }
}
