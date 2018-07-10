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
    /*
     * CQL Audit Log Entry Types
     */

    SELECT(AuditLogEntryCategory.QUERY),
    UPDATE(AuditLogEntryCategory.DML),
    DELETE(AuditLogEntryCategory.DML),
    TRUNCATE(AuditLogEntryCategory.DDL),
    CREATE_KEYSPACE(AuditLogEntryCategory.DDL),
    ALTER_KEYSPACE(AuditLogEntryCategory.DDL),
    DROP_KEYSPACE(AuditLogEntryCategory.DDL),
    CREATE_TABLE(AuditLogEntryCategory.DDL),
    DROP_TABLE(AuditLogEntryCategory.DDL),
    PREPARE_STATEMENT(AuditLogEntryCategory.PREPARE),
    DROP_TRIGGER(AuditLogEntryCategory.DDL),
    LIST_USERS(AuditLogEntryCategory.DCL),
    CREATE_INDEX(AuditLogEntryCategory.DDL),
    DROP_INDEX(AuditLogEntryCategory.DDL),
    GRANT(AuditLogEntryCategory.DCL),
    REVOKE(AuditLogEntryCategory.DCL),
    CREATE_TYPE(AuditLogEntryCategory.DDL),
    DROP_AGGREGATE(AuditLogEntryCategory.DDL),
    ALTER_VIEW(AuditLogEntryCategory.DDL),
    CREATE_VIEW(AuditLogEntryCategory.DDL),
    DROP_ROLE(AuditLogEntryCategory.DCL),
    DROP_USER(AuditLogEntryCategory.DCL),
    CREATE_FUNCTION(AuditLogEntryCategory.DDL),
    ALTER_TABLE(AuditLogEntryCategory.DDL),
    BATCH(AuditLogEntryCategory.DML),
    CREATE_AGGREGATE(AuditLogEntryCategory.DDL),
    DROP_VIEW(AuditLogEntryCategory.DDL),
    DROP_TYPE(AuditLogEntryCategory.DDL),
    DROP_FUNCTION(AuditLogEntryCategory.DDL),
    ALTER_ROLE(AuditLogEntryCategory.DCL),
    CREATE_TRIGGER(AuditLogEntryCategory.DDL),
    LIST_ROLES(AuditLogEntryCategory.DCL),
    LIST_PERMISSIONS(AuditLogEntryCategory.DCL),
    ALTER_TYPE(AuditLogEntryCategory.DDL),
    CREATE_ROLE(AuditLogEntryCategory.DCL),
    CREATE_USER(AuditLogEntryCategory.DCL),
    USE_KEYSPACE(AuditLogEntryCategory.OTHER),

    /*
     * Common Audit Log Entry Types
     */

    REQUEST_FAILURE(AuditLogEntryCategory.ERROR),
    LOGIN_ERROR(AuditLogEntryCategory.AUTH),
    UNAUTHORIZED_ATTEMPT(AuditLogEntryCategory.AUTH),
    LOGIN_SUCCESS(AuditLogEntryCategory.AUTH),


    /**
     * Thrift Audit Log Entry Types
     */
    GET(AuditLogEntryCategory.QUERY),
    GET_SLICE(AuditLogEntryCategory.QUERY),
    GET_COUNT(AuditLogEntryCategory.QUERY),
    MULTIGET_SLICE(AuditLogEntryCategory.QUERY),
    MULTIGET_COUNT(AuditLogEntryCategory.QUERY),
    GET_RANGE_SLICES(AuditLogEntryCategory.QUERY),
    GET_INDEXED_SLICES(AuditLogEntryCategory.QUERY),
    GET_PAGED_SLICE(AuditLogEntryCategory.QUERY),
    INSERT(AuditLogEntryCategory.DML),
    REMOVE(AuditLogEntryCategory.DML),
    ADD(AuditLogEntryCategory.DML),
    REMOVE_COUNTER(AuditLogEntryCategory.DML),
    BATCH_MUTATE(AuditLogEntryCategory.DML),
    SET_KS(AuditLogEntryCategory.OTHER),
    T_TRUNCATE(AuditLogEntryCategory.DDL),
    COMPARE_AND_SET(AuditLogEntryCategory.DML),
    DESC_KS(AuditLogEntryCategory.OTHER),
    ADD_CF(AuditLogEntryCategory.DDL),
    DROP_CF(AuditLogEntryCategory.DDL),
    UPDATE_CF(AuditLogEntryCategory.DDL),
    ADD_KS(AuditLogEntryCategory.DDL),
    T_DROP_KS(AuditLogEntryCategory.DDL),
    UPDATE_KS(AuditLogEntryCategory.DDL),
    LOGIN(AuditLogEntryCategory.AUTH),
    DESC_SCHEMA_VERSIONS(AuditLogEntryCategory.OTHER),
    DESC_CLUSTER_NAME(AuditLogEntryCategory.OTHER),
    DESC_VERSION(AuditLogEntryCategory.OTHER),
    DESC_RING(AuditLogEntryCategory.OTHER),
    DESC_LOCAL_RING(AuditLogEntryCategory.OTHER),
    DESC_TOKEN_MAP(AuditLogEntryCategory.OTHER),
    DESC_PARTITIONER(AuditLogEntryCategory.OTHER),
    DESC_SPLITS(AuditLogEntryCategory.OTHER),
    DESC_SNITCH(AuditLogEntryCategory.OTHER);



    private final AuditLogEntryCategory category;

    AuditLogEntryType(AuditLogEntryCategory category)
    {
        this.category = category;
    }

    public AuditLogEntryCategory getCategory()
    {
        return category;
    }
}
