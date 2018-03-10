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

import java.util.List;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.AlterKeyspaceStatement;
import org.apache.cassandra.cql3.statements.AlterTypeStatement;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateAggregateStatement;
import org.apache.cassandra.cql3.statements.CreateFunctionStatement;
import org.apache.cassandra.cql3.statements.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.DropAggregateStatement;
import org.apache.cassandra.cql3.statements.DropFunctionStatement;
import org.apache.cassandra.cql3.statements.DropKeyspaceStatement;
import org.apache.cassandra.cql3.statements.DropTypeStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;

public class AuditLogManager
{
    public static final Logger logger = LoggerFactory.getLogger(AuditLogManager.class);
    private static final AuditLogManager instance = new AuditLogManager();
    private final IAuditLogger auditLogger;
    private AuditLogManager()
    {
        if (isAuditingEnabled())
        {
            logger.info("Audit logging is enabled.");

        }
        else
        {
            logger.info("Audit logging is disabled.");
        }
        this.auditLogger = getAuditLogger();
    }

    public static AuditLogManager getInstance()
    {
        return instance;
    }


    private IAuditLogger getAuditLogger()
    {
        String loggerClassName = DatabaseDescriptor.getAuditLoggingOptions().logger;
        if(loggerClassName !=null)
        {
            return FBUtilities.newAuditLogger(loggerClassName);
        }
        return FBUtilities.newAuditLogger(FileAuditLogger.class.getName());
    }

    @VisibleForTesting
    public IAuditLogger getLogger()
    {
        return auditLogger;
    }

    public boolean isAuditingEnabled()
    {
        return DatabaseDescriptor.getAuditLoggingOptions().enabled;
    }

    private boolean isSystemKeyspace(String keyspaceName)
    {
        return SchemaConstants.isLocalSystemKeyspace(keyspaceName);
    }

    /**
     * Logging overloads
     */
    public void log(AuditLogEntry logEntry)
    {
        //TODO: Look into the design aspects of async based audit logging from LogManager(here) or leave it to implementers of IAuditLogger
        if (isAuditingEnabled()
            && (null != logEntry)
            && ((null == logEntry.getKeyspace()) || !isSystemKeyspace(logEntry.getKeyspace()))
            && (!AuditLogFilter.getInstance().isFiltered(logEntry)))
        {
            this.auditLogger.log(logEntry);
        }
    }

    public void log(List<AuditLogEntry> auditLogEntries)
    {
        for (AuditLogEntry auditLogEntry : auditLogEntries)
        {
            this.log(auditLogEntry);
        }
    }

    public void logError(AuditLogEntry logEntry)
    {
        if (isAuditingEnabled() && (null == logEntry.getKeyspace() || !isSystemKeyspace(logEntry.getKeyspace())))
        {
            this.auditLogger.error(logEntry);
        }
    }

    public void log(AuditLogEntry logEntry, Exception e)
    {
        if ((logEntry != null) && (this.isAuditingEnabled()))
        {
            AuditLogEntry auditEntry = new AuditLogEntry(logEntry);

            if (e instanceof UnauthorizedException)
            {
                auditEntry.setType(AuditLogEntryType.UNAUTHORIZED_ATTEMPT);
            }
            else if (e instanceof AuthenticationException)
            {
                auditEntry.setType(AuditLogEntryType.LOGIN_ERROR);
            }
            else
            {
                auditEntry.setType(AuditLogEntryType.REQUEST_FAILURE);
            }
            auditEntry.appendToOperation(e.getMessage());

            this.log(auditEntry);
        }
    }

    public void log(List<AuditLogEntry> auditLogEntries, Exception e)
    {
        if(null != auditLogEntries)
        {
            for (AuditLogEntry logEntry : auditLogEntries)
            {
                log(logEntry, e);
            }
        }
    }

    /**
     * Native protocol/ CQL helper methods for Audit Logging
     */

    public AuditLogEntry getLogEntry(CQLStatement statement, String queryString, QueryState queryState, AuditLogEntryType type)
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
     *
     * @param operation
     * @param queryState
     * @return
     */
    public AuditLogEntry getLogEntry(String operation, QueryState queryState, AuditLogEntryType type)
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

    public AuditLogEntry getLogEntry(CQLStatement statement, String queryString, QueryState queryState)
    {
        return this.getLogEntry(statement, queryString, queryState, statement.getAuditLogContext().auditLogEntryType);
    }

    public AuditLogEntry getLogEntry(CQLStatement statement, String queryString, QueryState queryState, QueryOptions queryOptions)
    {

        return this.getLogEntry(statement, queryString, queryState);
    }

    /**
     * Gets the AuditLogEntry entry as per the params given. Ensure that type is set by the caller.
     *
     * @param queryString
     * @param queryState
     * @param queryOptions
     * @return
     */
    public AuditLogEntry getLogEntry(String queryString, QueryState queryState, QueryOptions queryOptions)
    {

        AuditLogEntry entry = AuditLogEntry.getAuditEntry(queryState.getClientState());

        entry.setKeyspace(queryState.getClientState().getRawKeyspace())
             .setOperation(queryString);

        return entry;
    }

    /**
     * Gets the AuditLogEntry entry as per the params given. Ensure that type is set by the caller.
     *
     * @param queryString
     * @param queryState
     * @param queryOptions
     * @param batchId
     * @return
     */
    public AuditLogEntry getLogEntry(String queryString, QueryState queryState, QueryOptions queryOptions, UUID batchId)
    {
        AuditLogEntry entry = AuditLogEntry.getAuditEntry(queryState.getClientState());

        entry.setKeyspace(queryState.getClientState().getRawKeyspace())
             .setBatch(batchId)
             .setType(AuditLogEntryType.BATCH)
             .setOperation(queryString);
        return entry;
    }

    private AuditLogEntry getLogEntry(CQLStatement statement, String rawCQLStatement, QueryState queryState, QueryOptions queryOptions, UUID batchId)
    {
        return this.getLogEntry(statement, rawCQLStatement, queryState, queryOptions).setBatch(batchId);
    }

    public List<AuditLogEntry> getLogEntriesForBatch(List<Object> queryOrIdList, List<ParsedStatement.Prepared> prepared, QueryState state, QueryOptions options)
    {
        List<AuditLogEntry> auditLogEntries = Lists.newArrayList();

        UUID batchId = UUID.randomUUID();

        String queryString = String.format("BatchId:[%s] - BATCH of [%d] statements", batchId, queryOrIdList.size());

        auditLogEntries.add(this.getLogEntry(queryString, state, options, batchId));

        for (int i = 0; i < queryOrIdList.size(); i++)
        {
           auditLogEntries.add(this.getLogEntry(prepared.get(i).statement, prepared.get(i).rawCQLStatement, state, options, batchId));
        }

        return auditLogEntries;
    }

    /**
     * HELPER methods for Audit Logging
     */

    private String getKeyspace(CQLStatement stmt, QueryState queryState)
    {
        return stmt.getAuditLogContext().keyspace!=null ? stmt.getAuditLogContext().keyspace : queryState.getClientState().getRawKeyspace();
    }

    public static String getColumnFamily(CQLStatement stmt)
    {
        return stmt.getAuditLogContext().scope;
    }
}
