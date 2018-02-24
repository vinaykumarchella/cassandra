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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Date;
import java.util.UUID;

import com.google.common.base.Strings;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

public class AuditLogEntry
{

    private AuditLogEntryType type;
    private String user;
    private String source;
    private InetAddress host = FBUtilities.getJustBroadcastAddress();

    private long timestamp;
    private String keyspace;
    private String scope;
    private UUID batch;
    private String operation;
    private ConsistencyLevel consistencyLevel;

    public AuditLogEntry(String source, String user)
    {
        this.source = source;
        this.user = user;
        this.timestamp = new Date().getTime();
    }

    public AuditLogEntry(AuditLogEntryType type, String user, String source)
    {
        this(source, user);
        this.source = source;
        this.type = type;
    }

    public AuditLogEntry(AuditLogEntry event)
    {
        this(event.source, event.user);
        this.type = event.type;
        this.timestamp = event.timestamp;
        this.keyspace = event.keyspace;
        this.scope = event.scope;
        this.batch = event.batch;
        this.operation = event.operation;
        this.consistencyLevel = event.consistencyLevel;
    }


    public AuditLogEntry(AuditLogEntry event, UnauthorizedException e)
    {
        this(event);
        this.setType(AuditLogEntryType.UNAUTHORIZED_ATTEMPT);
        String operation = event.getOperation();
        operation = e.getLocalizedMessage() + (operation != null ? "; " + operation : "");
        this.setOperation(operation);
    }

    public AuditLogEntry(AuditLogEntry event, AuthenticationException e)
    {
        this(event);
        this.setType(AuditLogEntryType.LOGIN_ERROR);
        String operation = event.getOperation();
        operation = e.getLocalizedMessage() + (operation != null ? "; " + operation : "");
        this.setOperation(operation);
    }

    public static AuditLogEntry getAuditEntry(ClientState clientState)
    {
        if (clientState == null || clientState.getUser() == null)
        {
            return new AuditLogEntry(getSource(clientState), AuthenticatedUser.SYSTEM_USER.getName());
        }
        return new AuditLogEntry(getSource(clientState), clientState.getUser().getName());
    }

    private static String getSource(ClientState state)
    {
        SocketAddress sockAddress = state.getRemoteAddress();
        if (sockAddress == null)
        {
            return AuditLogUtil.DEFAULT_SOURCE.toString();
        }
        if (((sockAddress instanceof InetSocketAddress)) && (!((InetSocketAddress) sockAddress).isUnresolved()))
        {
            return ((InetSocketAddress) sockAddress).getAddress().toString();
        }
        return AuditLogUtil.DEFAULT_SOURCE.toString();
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder("host:").append(this.host).append("|source:").append(this.source).append("|user:").append(this.user).append("|timestamp:").append(this.timestamp).append("|type:").append(this.type);

        if (this.batch != null)
        {
            builder.append("|batch:").append(this.batch);
        }
        if (!Strings.isNullOrEmpty(this.keyspace))
        {
            builder.append("|ks:").append(this.keyspace);
        }
        if (!Strings.isNullOrEmpty(this.scope))
        {
            builder.append("|scope:").append(this.scope);
        }
        if (!Strings.isNullOrEmpty(this.operation))
        {
            builder.append("|operation:").append(this.operation);
        }
        if (this.consistencyLevel != null)
        {
            builder.append("|consistency level:").append(this.consistencyLevel);
        }
        return builder.toString();
    }

    public AuditLogEntry setUser(String user)
    {
        this.user = user;
        return this;
    }

    public AuditLogEntry setSource(String source)
    {
        this.source = source;
        return this;
    }

    public AuditLogEntry setHost(InetAddress host)
    {
        this.host = host;
        return this;
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

    public AuditLogEntry setConsistencyLevel(ConsistencyLevel consistencyLevel)
    {
        this.consistencyLevel = consistencyLevel;
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
}
