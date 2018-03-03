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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().includedKeyspaces.split(","))
        {
            if(!keyspace.isEmpty())
            {
                includedKeyspacesSet.add(keyspace);
            }
        }
        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().excludedKeyspaces.split(","))
        {
            if(!keyspace.isEmpty())
            {
                excludedKeyspacesSet.add(keyspace);
            }
        }

        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().includedCategories.split(","))
        {
            if(!keyspace.isEmpty())
            {
                includedCategoriesSet.add(keyspace);
            }
        }
        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().excludedCategories.split(","))
        {
            if(!keyspace.isEmpty())
            {
                excludedCategoriesSet.add(keyspace);
            }
        }

        includedKeyspaces.set(includedKeyspacesSet);
        excludedKeyspaces.set(excludedKeyspacesSet);

        includedCategories.set(includedCategoriesSet);
        excludedCategories.set(excludedCategoriesSet);
    }

    private boolean isFiltered(String keyspace)
    {
        if(keyspace != null && !keyspace.isEmpty())
        {
            boolean isExcluded = false;
            if(excludedKeyspaces.get() != null && excludedKeyspaces.get().size()>0)
            {
                isExcluded = excludedKeyspaces.get().contains(keyspace);
            }

            boolean isIncluded = true;
            if(includedKeyspaces.get() != null && includedKeyspaces.get().size()>0)
            {
                isIncluded = includedKeyspaces.get().contains(keyspace);
            }

            return isExcluded || (!isIncluded);
        }
        return false;
    }

    public boolean isFiltered(AuditLogEntry auditLogEntry)
    {
        return isFiltered(auditLogEntry.getKeyspace()) || isFiltered(auditLogEntry.getType());
    }

    private boolean isFiltered(AuditLogEntryType auditLogEntryType)
    {
        if(auditLogEntryType.getCategory() != null && !auditLogEntryType.getCategory().isEmpty())
        {
            boolean isExcluded = false;
            if(excludedCategories.get() != null && excludedCategories.get().size()>0)
            {
                isExcluded = excludedCategories.get().contains(auditLogEntryType.getCategory());
            }

            boolean isIncluded = true;
            if(includedCategories.get() != null && includedCategories.get().size()>0)
            {
                isIncluded = includedCategories.get().contains(auditLogEntryType.getCategory());
            }

            return isExcluded || (!isIncluded);
        }
        return false;
    }


}
