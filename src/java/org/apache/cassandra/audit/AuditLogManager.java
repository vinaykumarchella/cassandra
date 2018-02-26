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
import java.net.SocketAddress;
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
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

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

    public static InetSocketAddress getClientAddress(ClientState state)
    {
        SocketAddress sockAddress = state.getRemoteAddress();
        return (sockAddress instanceof InetSocketAddress) ? (InetSocketAddress) sockAddress : new InetSocketAddress(AuditLogUtil.DEFAULT_SOURCE, 0);
    }



    private IAuditLogger getAuditLogger()
    {
        String loggerClassName = DatabaseDescriptor.getAuditLoggingOptions().logger;
        if (!loggerClassName.contains("."))
        {
            loggerClassName = "org.apache.cassandra.audit." + loggerClassName;
        }
        logger.info("Logger instance loaded with class : " + loggerClassName);
        try
        {
            return (IAuditLogger) Class.forName(loggerClassName).newInstance();
        }
        catch (Exception e)
        {
            logger.error("Unable to load audit logger: " + loggerClassName, e);
            throw new RuntimeException(e);
        }
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

        // Filter keyspaces
        if (isAuditingEnabled()
            && null != logEntry
            && (null == logEntry.getKeyspace() || !isSystemKeyspace(logEntry.getKeyspace()))
            && (!AuditLogFilter.getInstance().isFiltered(logEntry.getKeyspace())))
        {
            this.auditLogger.log(logEntry);
        }
    }

    public void log(List<AuditLogEntry> events)
    {
        for (AuditLogEntry event : events)
        {
            this.log(event);
        }
    }

    public void logError(AuditLogEntry logEntry)
    {
        // Filter keyspaces
        if (isAuditingEnabled() && (null == logEntry.getKeyspace() || !isSystemKeyspace(logEntry.getKeyspace())))
        {
            this.auditLogger.error(logEntry);
        }
    }

    public void log(AuditLogEntry event, Exception e)
    {
        if ((event != null) && (this.isAuditingEnabled()))
        {
            AuditLogEntry auditEntry = new AuditLogEntry(event);

            auditEntry.setType(AuditLogEntryType.REQUEST_FAILURE);

            String operation = event.getOperation();
            operation = e.getLocalizedMessage() + (operation != null ? " - " + operation : "");

            auditEntry.setOperation(operation);

            this.log(auditEntry);
        }
    }

    public void log(List<AuditLogEntry> events, Exception e)
    {
        if(null != events)
        {
            for (AuditLogEntry event : events)
            {
                log(event, e);
            }
        }
    }

    public void log(AuditLogEntry auditLogEntry, ClientState clientState, Exception e)
    {
        if (this.isAuditingEnabled())
        {
            if (null == auditLogEntry)
            {
                auditLogEntry = AuditLogEntry.getAuditEntry(clientState);
            }
            if (e instanceof UnauthorizedException)
            {
                this.logUnauthorizedAttempt(auditLogEntry, (UnauthorizedException) e);
            }
            else if (e instanceof AuthenticationException)
            {
                this.logAuthenticationError(auditLogEntry, (AuthenticationException) e);
            }
            else
            {
                this.log(auditLogEntry, e);
            }
        }
    }

    public void logUnauthorizedAttempt(AuditLogEntry event, UnauthorizedException e)
    {
        if (this.isAuditingEnabled())
        {

            AuditLogEntry entry = new AuditLogEntry(event, e);
            this.logError(entry);
        }
    }

    public void logAuthenticationError(AuditLogEntry event, AuthenticationException e)
    {
        if (this.isAuditingEnabled())
        {
            AuditLogEntry entry = new AuditLogEntry(event, e);
            this.logError(entry);
        }
    }

    /**
     * Native protocol/ CQL helper methods for Audit Logging
     */

    public AuditLogEntry getEvent(CQLStatement statement, String queryString, QueryState queryState, AuditLogEntryType type)
    {
        AuditLogEntry entry = AuditLogEntry.getAuditEntry(queryState.getClientState());

        entry.setKeyspace(getKeyspace(statement, queryState))
             .setScope(getColumnFamily(statement))
             .setOperation(queryString)
             .setType(type);

        return entry;
    }

    /**
     * Gets the AuditEvent entry as per the params given. Ensure that type is set by the caller.
     *
     * @param operation
     * @param queryState
     * @return
     */
    public AuditLogEntry getEvent(String operation, QueryState queryState, AuditLogEntryType type)
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

    public AuditLogEntry getEvent(CQLStatement statement, String queryString, QueryState queryState)
    {
        return this.getEvent(statement, queryString, queryState, AuditLogEntryType.getType(statement));
    }

    public AuditLogEntry getEvent(CQLStatement statement, String queryString, QueryState queryState, QueryOptions queryOptions)
    {

        return this.getEvent(statement, queryString, queryState).setConsistencyLevel(queryOptions.getConsistency());
    }

    /**
     * Gets the AuditEvent entry as per the params given. Ensure that type is set by the caller.
     *
     * @param queryString
     * @param queryState
     * @param queryOptions
     * @return
     */
    public AuditLogEntry getEvent(String queryString, QueryState queryState, QueryOptions queryOptions)
    {

        AuditLogEntry entry = AuditLogEntry.getAuditEntry(queryState.getClientState());

        entry.setKeyspace(queryState.getClientState().getRawKeyspace())
             .setOperation(queryString)
             .setConsistencyLevel(queryOptions.getConsistency());

        return entry;
    }

    /**
     * Gets the AuditEvent entry as per the params given. Ensure that type is set by the caller.
     *
     * @param queryString
     * @param queryState
     * @param queryOptions
     * @param batchId
     * @return
     */
    public AuditLogEntry getEvent(String queryString, QueryState queryState, QueryOptions queryOptions, UUID batchId)
    {
        AuditLogEntry entry = AuditLogEntry.getAuditEntry(queryState.getClientState());

        entry.setKeyspace(queryState.getClientState().getRawKeyspace())
             .setBatch(batchId)
             .setOperation(queryString)
             .setConsistencyLevel(queryOptions.getConsistency());
        return entry;
    }

    private AuditLogEntry getEvent(CQLStatement statement, String rawCQLStatement, QueryState queryState, QueryOptions queryOptions, UUID batchId)
    {
        return this.getEvent(statement,rawCQLStatement,queryState,queryOptions).setBatch(batchId);
    }

    public List<AuditLogEntry> getEventsForBatch(List<Object> queryOrIdList, List<ParsedStatement.Prepared> prepared, QueryState state, QueryOptions options)
    {
        List<AuditLogEntry> events = Lists.newArrayList();

        UUID batchId = UUID.randomUUID();

        String queryString = String.format("BatchId:[%s] - BATCH of [%d] statements", batchId, queryOrIdList.size());

        events.add(this.getEvent(queryString, state, options, batchId).setType(AuditLogEntryType.BATCH));

        for (int i = 0; i < queryOrIdList.size(); i++)
        {
           events.add(this.getEvent(prepared.get(i).statement, prepared.get(i).rawCQLStatement, state, options, batchId));
        }

        return events;
    }

    /**
     * HELPER methods for Audit Logging
     */

    private String getKeyspace(CQLStatement stmt, QueryState queryState)
    {
        if ((stmt instanceof CreateFunctionStatement))
        {
            return ((CreateFunctionStatement)stmt).getFunctionName().keyspace;
        }
        if ((stmt instanceof DropFunctionStatement))
        {
            return ((DropFunctionStatement)stmt).getFunctionName().keyspace;
        }
        if ((stmt instanceof CreateAggregateStatement) )
        {
            return ((CreateAggregateStatement) stmt).getFunctionName().keyspace;
        }
        if ((stmt instanceof DropAggregateStatement) )
        {
            return ((DropAggregateStatement) stmt).getFunctionName().keyspace;
        }
        if ((stmt instanceof CFStatement))
        {
            return ((CFStatement) stmt).keyspace();
        }
        if ((stmt instanceof ModificationStatement))
        {
            return ((ModificationStatement) stmt).keyspace();
        }
        if ((stmt instanceof SelectStatement))
        {
            return ((SelectStatement) stmt).keyspace();
        }

        return queryState.getClientState().getRawKeyspace();
    }

    public static String getColumnFamily(CQLStatement stmt)
    {

        if (((stmt instanceof DropKeyspaceStatement)) || ((stmt instanceof CreateKeyspaceStatement)) || ((stmt instanceof AlterKeyspaceStatement)))
        {
            return null;
        }
        if ((stmt instanceof CreateFunctionStatement) )
        {
            return ((CreateFunctionStatement) stmt).getFunctionName().name;
        }
        if ((stmt instanceof DropFunctionStatement) )
        {
            return ((DropFunctionStatement) stmt).getFunctionName().name;
        }
        if ((stmt instanceof CreateAggregateStatement) )
        {
            return ((CreateAggregateStatement) stmt).getFunctionName().name;
        }
        if ((stmt instanceof DropAggregateStatement) )
        {
            return ((DropAggregateStatement) stmt).getFunctionName().name;
        }
        if ((stmt instanceof CreateTypeStatement) )
        {
            return ((CreateTypeStatement) stmt).getStringTypeName();
        }
        if ((stmt instanceof AlterTypeStatement) )
        {
            return ((AlterTypeStatement) stmt).getStringTypeName();
        }
        if ( (stmt instanceof DropTypeStatement))
        {
            return ((DropTypeStatement) stmt).getStringTypeName();
        }
        if ((stmt instanceof CFStatement))
        {
            return ((CFStatement) stmt).columnFamily();
        }
        if ((stmt instanceof ModificationStatement))
        {
            return ((ModificationStatement) stmt).columnFamily();
        }
        if ((stmt instanceof SelectStatement))
        {
            return ((SelectStatement) stmt).table.name;
        }
        return null;
    }
}
