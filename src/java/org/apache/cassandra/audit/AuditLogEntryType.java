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

    private static final Map<String, AuditLogEntryType> allStatementsMap = new HashMap<>();
    final String category;

    AuditLogEntryType(String category) {
        this.category = category;
    }

    @VisibleForTesting
    public static Map<String, AuditLogEntryType> getAllStatementsMap()
    {
        return allStatementsMap;
    }

    public static AuditLogEntryType getType(CQLStatement statement)
    {
        AuditLogEntryType returnType = allStatementsMap.getOrDefault(statement.getClass().getCanonicalName(), AuditLogEntryType.UNKNOWN);

        if (returnType == AuditLogEntryType.UNKNOWN)
        {
            returnType = allStatementsMap.getOrDefault(getParentClassName(statement.getClass().getTypeName()), AuditLogEntryType.UNKNOWN);
        }

        return returnType;
    }

    @VisibleForTesting
    public static String getParentClassName(String typeName)
    {
        // Check if the class is of any of following types, if so get the parent class name
        // Static inner class, Private Static inner class, Inner class,
        // Anonymous class, Lambda runnable class, Method reference class
        if(typeName.contains("$"))
        {
            String[] classNameSplit = typeName.split("\\$");
            if(classNameSplit.length>0)
            {
                return classNameSplit[0];
            }
        }
        return null;
    }

    public String getCategory()
    {
        return this.category;
    }

    static
    {
        allStatementsMap.put(SelectStatement.class.getCanonicalName(), AuditLogEntryType.SELECT);
        allStatementsMap.put(DropTriggerStatement.class.getCanonicalName(), AuditLogEntryType.DROP_TRIGGER);
        allStatementsMap.put(GrantPermissionsStatement.class.getCanonicalName(), AuditLogEntryType.GRANT);
        allStatementsMap.put(CreateTypeStatement.class.getCanonicalName(), AuditLogEntryType.CREATE_TYPE);
        allStatementsMap.put(DropKeyspaceStatement.class.getCanonicalName(), AuditLogEntryType.DROP_KS);
        allStatementsMap.put(DeleteStatement.class.getCanonicalName(), AuditLogEntryType.DELETE);
        allStatementsMap.put(DropAggregateStatement.class.getCanonicalName(), AuditLogEntryType.DROP_AGG);
        allStatementsMap.put(UpdateStatement.class.getCanonicalName(), AuditLogEntryType.UPDATE);
        allStatementsMap.put(AlterViewStatement.class.getCanonicalName(), AuditLogEntryType.ALTER_VIEW);
        allStatementsMap.put(CreateViewStatement.class.getCanonicalName(), AuditLogEntryType.CREATE_VIEW);
        allStatementsMap.put(DropRoleStatement.class.getCanonicalName(), AuditLogEntryType.DROP_ROLE);
        allStatementsMap.put(UseStatement.class.getCanonicalName(), AuditLogEntryType.USE_KS);
        allStatementsMap.put(RevokeRoleStatement.class.getCanonicalName(), AuditLogEntryType.REVOKE);
        allStatementsMap.put(CreateFunctionStatement.class.getCanonicalName(), AuditLogEntryType.CREATE_FUNC);
        allStatementsMap.put(AlterTableStatement.class.getCanonicalName(), AuditLogEntryType.ALTER_TABLE);
        allStatementsMap.put(BatchStatement.class.getCanonicalName(), AuditLogEntryType.BATCH);
        allStatementsMap.put(CreateAggregateStatement.class.getCanonicalName(), AuditLogEntryType.CREATE_AGG);
        allStatementsMap.put(CreateIndexStatement.class.getCanonicalName(), AuditLogEntryType.CREATE_INDEX);
        allStatementsMap.put(AuthenticationStatement.class.getCanonicalName(), AuditLogEntryType.AUTH);
        allStatementsMap.put(DropIndexStatement.class.getCanonicalName(), AuditLogEntryType.DROP_INDEX);
        allStatementsMap.put(CreateKeyspaceStatement.class.getCanonicalName(), AuditLogEntryType.CREATE_KS);
        allStatementsMap.put(ListUsersStatement.class.getCanonicalName(), AuditLogEntryType.LIST_USERS);
        allStatementsMap.put(AlterKeyspaceStatement.class.getCanonicalName(), AuditLogEntryType.ALTER_KS);
        allStatementsMap.put(DropViewStatement.class.getCanonicalName(), AuditLogEntryType.DROP_VIEW);
        allStatementsMap.put(DropTypeStatement.class.getCanonicalName(), AuditLogEntryType.DROP_TYPE);
        allStatementsMap.put(GrantRoleStatement.class.getCanonicalName(), AuditLogEntryType.GRANT);
        allStatementsMap.put(DropFunctionStatement.class.getCanonicalName(), AuditLogEntryType.DROP_FUNC);
        allStatementsMap.put(AlterRoleStatement.class.getCanonicalName(), AuditLogEntryType.ALTER_ROLE);
        allStatementsMap.put(SelectStatement.class.getCanonicalName(), AuditLogEntryType.SELECT);
        allStatementsMap.put(TruncateStatement.class.getCanonicalName(), AuditLogEntryType.TRUNCATE);
        allStatementsMap.put(CreateTriggerStatement.class.getCanonicalName(), AuditLogEntryType.CREATE_TRIGGER);
        allStatementsMap.put(DropTableStatement.class.getCanonicalName(), AuditLogEntryType.DROP_TABLE);
        allStatementsMap.put(RevokePermissionsStatement.class.getCanonicalName(), AuditLogEntryType.REVOKE);
        allStatementsMap.put(ListRolesStatement.class.getCanonicalName(), AuditLogEntryType.LIST_ROLES);
        allStatementsMap.put(ListPermissionsStatement.class.getCanonicalName(), AuditLogEntryType.LIST_PERMISSIONS);
        allStatementsMap.put(AlterTypeStatement.class.getCanonicalName(), AuditLogEntryType.ALTER_TYPE);
        allStatementsMap.put(CreateRoleStatement.class.getCanonicalName(), AuditLogEntryType.CREATE_ROLE);
        allStatementsMap.put(CreateTableStatement.class.getCanonicalName(), AuditLogEntryType.CREATE_TABLE);
    }

}
