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

import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.AlterKeyspaceStatement;
import org.apache.cassandra.cql3.statements.AlterRoleStatement;
import org.apache.cassandra.cql3.statements.AlterTableStatement;
import org.apache.cassandra.cql3.statements.AlterTypeStatement;
import org.apache.cassandra.cql3.statements.AlterViewStatement;
import org.apache.cassandra.cql3.statements.AuthenticationStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.CreateAggregateStatement;
import org.apache.cassandra.cql3.statements.CreateFunctionStatement;
import org.apache.cassandra.cql3.statements.CreateIndexStatement;
import org.apache.cassandra.cql3.statements.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.CreateRoleStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.CreateTriggerStatement;
import org.apache.cassandra.cql3.statements.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.CreateViewStatement;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.DropAggregateStatement;
import org.apache.cassandra.cql3.statements.DropFunctionStatement;
import org.apache.cassandra.cql3.statements.DropIndexStatement;
import org.apache.cassandra.cql3.statements.DropKeyspaceStatement;
import org.apache.cassandra.cql3.statements.DropRoleStatement;
import org.apache.cassandra.cql3.statements.DropTableStatement;
import org.apache.cassandra.cql3.statements.DropTriggerStatement;
import org.apache.cassandra.cql3.statements.DropTypeStatement;
import org.apache.cassandra.cql3.statements.DropViewStatement;
import org.apache.cassandra.cql3.statements.GrantPermissionsStatement;
import org.apache.cassandra.cql3.statements.GrantRoleStatement;
import org.apache.cassandra.cql3.statements.ListPermissionsStatement;
import org.apache.cassandra.cql3.statements.ListRolesStatement;
import org.apache.cassandra.cql3.statements.ListUsersStatement;
import org.apache.cassandra.cql3.statements.RevokePermissionsStatement;
import org.apache.cassandra.cql3.statements.RevokeRoleStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.cql3.statements.UseStatement;

public enum AuditLogEntryType
{
    /**
     * CQL Audit Log Entry Types
     */

    SELECT("QUERY"),
    UPDATE("DML"),
    DELETE("DML"),
    TRUNCATE("DDL"),
    CREATE_KS("DDL"),
    ALTER_KS("DDL"),
    DROP_KS("DDL"),
    CREATE_TABLE("DDL"),
    DROP_TABLE("DDL"),
    PREPARE_STATEMENT("DML"),
    DROP_TRIGGER("DDL"),
    LIST_USERS("DCL"),
    CREATE_INDEX("DDL"),
    DROP_INDEX("DDL"),
    GRANT("DCL"),
    REVOKE("DCL"),
    CREATE_TYPE("DDL"),
    DROP_AGG("DDL"),
    ALTER_VIEW("DDL"),
    CREATE_VIEW("DDL"),
    DROP_ROLE("DCL"),
    CREATE_FUNC("DDL"),
    ALTER_TABLE("DDL"),
    BATCH("DML"),
    CREATE_AGG("DDL"),
    AUTH("AUTH"),
    DROP_VIEW("DDL"),
    DROP_TYPE("DDL"),
    DROP_FUNC("DDL"),
    ALTER_ROLE("DCL"),
    CREATE_TRIGGER("DDL"),
    LIST_ROLES("DCL"),
    LIST_PERMISSIONS("DCL"),
    ALTER_TYPE("DDL"),
    CREATE_ROLE("DCL"),
    USE_KS("DDL"),


    /**
     * Common Audit Log Entry Types
     */

    REQUEST_FAILURE("Other"),
    LOGIN_ERROR("AUTH"),
    UNAUTHORIZED_ATTEMPT("AUTH"),
    UNKNOWN("Other"),
    LOGIN_SUCCESS("AUTH");


    final String category;

    AuditLogEntryType(String category) {
        this.category = category;
    }

    public String getCategory()
    {
        return this.category;
    }

}
