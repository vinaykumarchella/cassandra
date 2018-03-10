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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

public class AuditLogFilter
{
    public static final Logger logger = LoggerFactory.getLogger(AuditLogFilter.class);

    private final AtomicReference<Set<String>> excludedKeyspaces = new AtomicReference<>();
    private final AtomicReference<Set<String>> includedKeyspaces = new AtomicReference<>();

    private final AtomicReference<Set<String>> excludedCategories = new AtomicReference<>();
    private final AtomicReference<Set<String>> includedCategories = new AtomicReference<>();

    private final AtomicReference<Set<String>> includedUsers = new AtomicReference<>();
    private final AtomicReference<Set<String>> excludedUsers= new AtomicReference<>();


    private static final AuditLogFilter instance = new AuditLogFilter();

    public AuditLogFilter()
    {
       loadFilters();
    }

    public static AuditLogFilter getInstance()
    {
     return instance;
    }

    public void loadFilters()
    {
        logger.info("Loading AuditLog filters");
        Set<String> includedKeyspacesSet = new HashSet<>();
        Set<String> excludedKeyspacesSet = new HashSet<>();

        Set<String> excludedCategoriesSet = new HashSet<>();
        Set<String> includedCategoriesSet = new HashSet<>();


        Set<String> excludedUsersSet = new HashSet<>();
        Set<String> includedUsersSet = new HashSet<>();

        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().included_keyspaces.split(","))
        {
            if(!keyspace.isEmpty())
            {
                includedKeyspacesSet.add(keyspace);
            }
        }
        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().excluded_keyspaces.split(","))
        {
            if(!keyspace.isEmpty())
            {
                excludedKeyspacesSet.add(keyspace);
            }
        }

        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().included_categories.split(","))
        {
            if(!keyspace.isEmpty())
            {
                includedCategoriesSet.add(keyspace);
            }
        }
        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().excluded_categories.split(","))
        {
            if(!keyspace.isEmpty())
            {
                excludedCategoriesSet.add(keyspace);
            }
        }

        for (String user : DatabaseDescriptor.getAuditLoggingOptions().included_users.split(","))
        {
            if(!user.isEmpty())
            {
                includedUsersSet.add(user);
            }
        }
        for (String user : DatabaseDescriptor.getAuditLoggingOptions().excluded_users.split(","))
        {
            if(!user.isEmpty())
            {
                excludedUsersSet.add(user);
            }
        }

        includedKeyspaces.set(includedKeyspacesSet);
        excludedKeyspaces.set(excludedKeyspacesSet);

        includedCategories.set(includedCategoriesSet);
        excludedCategories.set(excludedCategoriesSet);

        includedUsers.set(includedUsersSet);
        excludedUsers.set(excludedUsersSet);
    }

    public boolean isFiltered(AuditLogEntry auditLogEntry)
    {
        return isFiltered(auditLogEntry.getKeyspace(),includedKeyspaces,excludedKeyspaces)
               || isFiltered(auditLogEntry.getType().getCategory(),includedCategories,excludedCategories)
               || isFiltered(auditLogEntry.getUser(),includedUsers,excludedUsers);
    }

    private boolean isFiltered(String input, AtomicReference<Set<String>> includeSet, AtomicReference<Set<String>> excludeSet)
    {
        if (input != null && !input.isEmpty())
        {
            boolean isExcluded = false;
            if (excludeSet.get() != null && excludeSet.get().size() > 0)
            {
                isExcluded = excludeSet.get().contains(input);
            }
            if (isExcluded)
            {
                return true;
            }
            else
            {
                boolean isIncluded = true;
                if (includeSet.get() != null && includeSet.get().size() > 0)
                {
                    isIncluded = includeSet.get().contains(input);
                }
                return !isIncluded;
            }
        }
        return false;
    }

}
