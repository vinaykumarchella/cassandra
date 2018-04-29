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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Central location for managing the logging of client/user-initated actions (like queries, log in commands, and so on).
 *
 * We can run multiple {@link IAuditLogger}s at the same time, including the standard audit logger and the fq logger.
 */
public class AuditLogManager
{
    private static final Logger logger = LoggerFactory.getLogger(AuditLogManager.class);
    private static final AuditLogManager instance = new AuditLogManager();

    // FQL always writes to a BinLog, but it is a type of IAuditLogger
    private final FullQueryLogger fullQueryLogger;

    // auditLogger can write anywhere, as it's pluggable (logback, BinLog, DiagnosticEvents, etc ...)
    private volatile IAuditLogger auditLogger;

    private volatile AuditLogFilter filter;
    private volatile boolean isAuditLogEnabled;

    private AuditLogManager()
    {
        fullQueryLogger = new FullQueryLogger();

        if (DatabaseDescriptor.getAuditLoggingOptions().enabled)
        {
            logger.info("Audit logging is enabled.");
            auditLogger = getAuditLogger(DatabaseDescriptor.getAuditLoggingOptions().logger);
            isAuditLogEnabled = true;
        }
        else
        {
            logger.info("Audit logging is disabled.");
            isAuditLogEnabled = false;
            auditLogger = new NoOpAuditLogger();
        }

        filter = AuditLogFilter.create(DatabaseDescriptor.getAuditLoggingOptions());
    }

    public static AuditLogManager getInstance()
    {
        return instance;
    }

    private IAuditLogger getAuditLogger(String loggerClassName) throws ConfigurationException
    {
        if (loggerClassName != null)
        {
            return FBUtilities.newAuditLogger(loggerClassName);
        }

        return FBUtilities.newAuditLogger(BinAuditLogger.class.getName());
    }

    @VisibleForTesting
    public IAuditLogger getLogger()
    {
        return auditLogger;
    }

    public boolean isAuditingEnabled()
    {
        return this.isAuditLogEnabled;
    }

    public boolean isLoggingEnabled()
    {
        return isAuditingEnabled() || isFQLEnabled();
    }

    private boolean isFQLEnabled()
    {
        return fullQueryLogger.enabled();
    }

    private boolean isSystemKeyspace(String keyspaceName)
    {
        return SchemaConstants.isLocalSystemKeyspace(keyspaceName);
    }

    public void log(AuditLogEntry logEntry)
    {
        if (logEntry == null)
            return;

        if (isAuditingEnabled()
            && (logEntry.getKeyspace() == null || !isSystemKeyspace(logEntry.getKeyspace()))
            && !filter.isFiltered(logEntry))
        {
            auditLogger.log(logEntry);
        }

        if (isFQLEnabled())
        {
            fullQueryLogger.log(logEntry);
        }
    }

    public void log(AuditLogEntry logEntry, Exception e)
    {
        if ((logEntry != null) && (isAuditingEnabled()))
        {
            AuditLogEntry.Builder builder = new AuditLogEntry.Builder(logEntry);

            if (e instanceof UnauthorizedException)
            {
                builder.setType(AuditLogEntryType.UNAUTHORIZED_ATTEMPT);
            }
            else if (e instanceof AuthenticationException)
            {
                builder.setType(AuditLogEntryType.LOGIN_ERROR);
            }
            else
            {
                builder.setType(AuditLogEntryType.REQUEST_FAILURE);
            }

            builder.appendToOperation(e.getMessage());

            log(builder.build());
        }
    }

    public void logBatch(String batchTypeName, List<Object> queryOrIdList, List<List<ByteBuffer>> values, List<ParsedStatement.Prepared> prepared, QueryOptions options, QueryState state, long queryStartNanoTime)
    {
        if (!(isAuditingEnabled() || isFQLEnabled()))
            return;

        if (isAuditingEnabled())
        {
            List<AuditLogEntry> entries = buildEntriesForBatch(queryOrIdList, prepared, state, options);
            for (AuditLogEntry auditLogEntry : entries)
            {
                log(auditLogEntry);
            }
        }

        if (isFQLEnabled())
        {
            List<String> queryStrings = new ArrayList<>(queryOrIdList.size());
            for (ParsedStatement.Prepared prepStatment : prepared)
            {
                queryStrings.add(prepStatment.rawCQLStatement);
            }
            fullQueryLogger.logBatch(batchTypeName, queryStrings, values, options, queryStartNanoTime);
        }
    }

    private static List<AuditLogEntry> buildEntriesForBatch(List<Object> queryOrIdList, List<ParsedStatement.Prepared> prepared, QueryState state, QueryOptions options)
    {
        List<AuditLogEntry> auditLogEntries = new ArrayList<>(queryOrIdList.size() + 1);
        UUID batchId = UUID.randomUUID();
        String queryString = String.format("BatchId:[%s] - BATCH of [%d] statements", batchId, queryOrIdList.size());
        AuditLogEntry entry = new AuditLogEntry.Builder(state.getClientState())
                              .setOperation(queryString)
                              .setOptions(options)
                              .setBatch(batchId)
                              .build();
        auditLogEntries.add(entry);

        for (int i = 0; i < queryOrIdList.size(); i++)
        {
            CQLStatement statement = prepared.get(i).statement;
            entry = new AuditLogEntry.Builder(state.getClientState())
                    .setType(statement.getAuditLogContext().auditLogEntryType)
                    .setOperation(prepared.get(i).rawCQLStatement)
                    .setScope(statement)
                    .setKeyspace(state, statement)
                    .setOptions(options)
                    .setBatch(batchId)
                    .build();
            auditLogEntries.add(entry);
        }

        return auditLogEntries;
    }

    /**
     * Disables AuditLog, this is supposed to be used only via JMX/ Nodetool. Not designed to call from anywhere else in the codepath
     */
    public synchronized void disableAuditLog()
    {
        logger.info("Audit logging is disabled, stopping any existing loggers");
        if (this.isAuditingEnabled())
        {
            /*
             * Disable isAuditLogEnabled before attempting to cleanup/ stop AuditLogger so that any incoming log() requests
             * would be filtered. To avoid further race conditions, this.auditLogger is swapped with No-Op implementation
             * of IAuditLogger.
             */

            this.isAuditLogEnabled = false;
            IAuditLogger oldLogger = auditLogger;
            /*
             * Avoid race conditions by passing NoOpAuditLogger while disabling auditlog via nodetool
             */
            auditLogger = new NoOpAuditLogger();
            oldLogger.stop();
        }
    }

    /**
     * Enables AuditLog, this is supposed to be used only via JMX/ Nodetool. Not designed to call from anywhere else in the codepath
     * @param auditLogOptions AuditLogOptions to be used for enabling AuditLog
     * @throws ConfigurationException It can throw configuration exception when provided logger class does not exist in the classpath
     */
    public synchronized void enableAuditLog(AuditLogOptions auditLogOptions) throws ConfigurationException
    {
        logger.debug("Audit logging is being enabled. Reloading AuditLogOptions.");
        IAuditLogger oldLogger = auditLogger;

        filter = AuditLogFilter.create(auditLogOptions);

        if (oldLogger != null && oldLogger.getClass().getSimpleName().equals(auditLogOptions.logger))
        {
            logger.info("New AuditLogger [{}] is same as existing logger, hence not initializing the logger", auditLogOptions.logger);
            return;
        }

        auditLogger = getAuditLogger(auditLogOptions.logger);

        /* Ensure oldLogger's stop() is called after we swap it with new logger otherwise,
         * we might be calling log() on the stopped logger.
         */
        if (oldLogger != null)
        {
            oldLogger.stop();
        }
        this.isAuditLogEnabled = true;
        logger.debug("Audit logging is enabled. Reloaded AuditLogOptions.");
    }

    public void configureFQL(Path path, String rollCycle, boolean blocking, int maxQueueWeight, long maxLogSize)
    {
        fullQueryLogger.configure(path, rollCycle, blocking, maxQueueWeight, maxLogSize);
    }

    public void resetFQL(String fullQueryLogPath)
    {
        fullQueryLogger.reset(fullQueryLogPath);
    }

    public void disableFQL()
    {
        fullQueryLogger.stop();
    }

    /**
     * ONLY FOR TESTING
     */
    FullQueryLogger getFullQueryLogger()
    {
        return fullQueryLogger;
    }
}
