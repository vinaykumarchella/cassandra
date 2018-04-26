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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.fullquerylog.FullQueryLogger;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;

public class AuditLogManager
{
    private static final Logger logger = LoggerFactory.getLogger(AuditLogManager.class);
    private static final AuditLogManager instance = new AuditLogManager();

    private IAuditLogger auditLogger;
    private volatile AuditLogFilter filter;
    private AuditLogManager()
    {
        if (DatabaseDescriptor.getAuditLoggingOptions().enabled)
        {
            logger.info("Audit logging is enabled.");
            this.auditLogger = getAuditLogger(DatabaseDescriptor.getAuditLoggingOptions().logger);
//            this.isAuditLogEnabled = true;
        }
        else
        {
            logger.info("Audit logging is disabled.");
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
        return this.auditLogger != null;
    }

    public boolean isLoggingEnabled()
    {
        return isAuditingEnabled() || isFQLEnabled();
    }

    private boolean isFQLEnabled()
    {
        return FullQueryLogger.instance.enabled();
    }

    private boolean isSystemKeyspace(String keyspaceName)
    {
        return SchemaConstants.isLocalSystemKeyspace(keyspaceName);
    }

    /*
     * Logging overloads
     */
    public void log(AuditLogEntry logEntry)
    {
        if (isAuditingEnabled()
            && (null != auditLogger)
            && (null != logEntry)
            && ((null == logEntry.getKeyspace()) || !isSystemKeyspace(logEntry.getKeyspace()))
            && (!filter.isFiltered(logEntry)))
        {
            auditLogger.log(logEntry);
        }
    }

    public void log(List<AuditLogEntry> auditLogEntries)
    {
        for (AuditLogEntry auditLogEntry : auditLogEntries)
        {
            log(auditLogEntry);
        }
    }

    public void log(AuditLogEntry logEntry, Exception e)
    {
        if ((logEntry != null) && (isAuditingEnabled()))
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

            log(auditEntry);
        }
    }

    public void log(List<AuditLogEntry> auditLogEntries, Exception e)
    {
        if (null != auditLogEntries)
        {
            for (AuditLogEntry logEntry : auditLogEntries)
            {
                log(logEntry, e);
            }
        }
    }

    public void log(CQLStatement statement, String query, QueryOptions options, QueryState state, long queryStartNanoTime)
    {
        /**
         * We can run both the audit logger and the fq logger at the same time, hence this method ensures that it logs
         * to both the channels at same time.
         */
        if (isAuditingEnabled())
        {
            AuditLogEntry auditEntry = AuditLogEntry.getLogEntry(statement, query, state, options);
            this.log(auditEntry);
        }

        if (isFQLEnabled())
        {
            long fqlTime = System.currentTimeMillis() - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - queryStartNanoTime);
            FullQueryLogger.instance.logQuery(query, options, fqlTime);
        }
    }

    public void logBatch(String batchTypeName, List<Object> queryOrIdList, List<List<ByteBuffer>> values, List<ParsedStatement.Prepared> prepared, QueryOptions options, QueryState state, long queryStartNanoTime)
    {
        if (isAuditingEnabled())
        {
            log(AuditLogEntry.getLogEntriesForBatch(queryOrIdList, prepared, state, options));
        }

        if (isFQLEnabled())
        {
            List<String> queryStrings = new ArrayList<>(queryOrIdList.size());
            for (ParsedStatement.Prepared prepStatment : prepared)
            {
                queryStrings.add(prepStatment.rawCQLStatement);
            }
            FullQueryLogger.instance.logBatch(batchTypeName, queryStrings, values, options, queryStartNanoTime);
        }
    }

    public synchronized void disableAuditLog()
    {
        logger.info("Audit logging is disabled, stopping any existing loggers"); //debug
        if (this.auditLogger != null)
        {
            this.auditLogger.stop();
            this.auditLogger = null;
        }
    }

    public synchronized void enableAuditLog(AuditLogOptions auditLogOptions) throws ConfigurationException
    {
        logger.trace("Audit logging is being enabled. Reloading AuditLogOptions.");
        IAuditLogger oldLogger = this.auditLogger;
        filter = AuditLogFilter.create(auditLogOptions);

        if (oldLogger != null && oldLogger.getClass().getSimpleName().equals(auditLogOptions.logger))
        {
            logger.info("New AuditLogger [{}] is same as existing logger, hence not initializing the logger", auditLogOptions.logger);
            return;
        }

        this.auditLogger = getAuditLogger(auditLogOptions.logger);

            /* Ensure oldLogger's stop() is called after we swap it with new logger otherwise,
             * we might be calling log() on the stopped logger.
             */

        if (oldLogger != null)
        {
            oldLogger.stop();
        }
        logger.debug("Audit logging is enabled. Reloaded AuditLogOptions.");
    }
}
