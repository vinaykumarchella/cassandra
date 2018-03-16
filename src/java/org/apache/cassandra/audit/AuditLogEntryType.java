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
