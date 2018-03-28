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
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

public class AuditLogEntry
{

    private final InetAddressAndPort host = FBUtilities.getBroadcastAddressAndPort();
    private final String source, user;
    private final int srcPort;
    private long timestamp;
    private AuditLogEntryType type;
    private UUID batch;
    private String keyspace, scope, operation="";

    public AuditLogEntry(String source, int srcPort, String user)
    {
        this.source = source;
        this.user = user;
        this.srcPort = srcPort;
        this.timestamp = System.currentTimeMillis();
    }

    public AuditLogEntry(AuditLogEntryType type, String user, String source, int srcPort)
    {
        this(source, srcPort, user);
        this.type = type;
    }

    public AuditLogEntry(AuditLogEntry auditLogEntry)
    {
        this(auditLogEntry.type, auditLogEntry.user, auditLogEntry.source, auditLogEntry.srcPort);
        this.timestamp = auditLogEntry.timestamp;
        this.keyspace = auditLogEntry.keyspace;
        this.scope = auditLogEntry.scope;
        this.batch = auditLogEntry.batch;
        this.operation = auditLogEntry.operation;
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
        StringBuilder builder = new StringBuilder();
        builder.append("user:").append(this.user)
               .append("|host:").append(this.host)
               .append("|source:").append(this.source);
        if (srcPort > 0)
        {
            builder.append("|port:").append(this.srcPort);
        }
        builder.append("|timestamp:").append(this.timestamp)
               .append("|type:").append(this.type)
               .append("|category:").append(this.type.getCategory());

        if (this.batch != null)
        {
            builder.append("|batch:").append(this.batch);
        }
        if (this.keyspace != null && this.keyspace.length() > 0)
        {
            builder.append("|ks:").append(this.keyspace);
        }
        if (this.scope != null && this.scope.length() > 0)
        {
            builder.append("|scope:").append(this.scope);
        }
        if (this.operation != null && StringUtils.isNotBlank(this.operation))
        {
            builder.append("|operation:").append(this.operation);
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
        if(StringUtils.isNotBlank(str))
        {
            this.operation = this.operation.concat("; ").concat(str);
        }
    }
    private static InetSocketAddress getSource(ClientState state)
    {
        if (state!=null && state.getRemoteAddress() != null)
        {
            return state.getRemoteAddress();
        }
        return AuditLogUtil.DEFAULT_SOURCE;
    }
}
