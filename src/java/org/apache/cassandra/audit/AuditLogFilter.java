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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

public class AuditLogFilter
{
    public static final Logger logger = LoggerFactory.getLogger(AuditLogFilter.class);

    private final AtomicReference<List<String>> excludedKeyspaces = new AtomicReference<>();
    private final AtomicReference<List<String>> includedKeyspaces = new AtomicReference<>();

    private final AtomicReference<List<String>> excludedCategories = new AtomicReference<>();
    private final AtomicReference<List<String>> includedCategories = new AtomicReference<>();


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
        List<String> includedKeyspacesList = new ArrayList<>();
        List<String> excludedKeyspacesList = new ArrayList<>();

        List<String> excludedCategoriesList = new ArrayList<>();
        List<String> includedCategoriesList = new ArrayList<>();

        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().includedKeyspaces.split(","))
        {
            if(!keyspace.isEmpty())
            {
                includedKeyspacesList.add(keyspace);
            }
        }
        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().excludedKeyspaces.split(","))
        {
            if(!keyspace.isEmpty())
            {
                excludedKeyspacesList.add(keyspace);
            }
        }

        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().includedCategories.split(","))
        {
            if(!keyspace.isEmpty())
            {
                includedCategoriesList.add(keyspace);
            }
        }
        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().excludedCategories.split(","))
        {
            if(!keyspace.isEmpty())
            {
                excludedCategoriesList.add(keyspace);
            }
        }

        includedKeyspaces.set(includedKeyspacesList);
        excludedKeyspaces.set(excludedKeyspacesList);

        includedCategories.set(includedCategoriesList);
        excludedCategories.set(excludedCategoriesList);
    }

    private boolean isFiltered(String keyspace)
    {
        if(keyspace != null && !keyspace.isEmpty())
        {
            boolean isExcluded = false;
            if(excludedKeyspaces.get() != null && excludedKeyspaces.get().size()>0)
            {
                isExcluded = excludedKeyspaces.get().stream().anyMatch(s -> s.equalsIgnoreCase(keyspace));
            }

            boolean isIncluded = true;
            if(includedKeyspaces.get() != null && includedKeyspaces.get().size()>0)
            {
                isIncluded = includedKeyspaces.get().stream().anyMatch(s -> s.equalsIgnoreCase(keyspace));
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
                isExcluded = excludedCategories.get().stream().anyMatch(s -> s.equalsIgnoreCase(auditLogEntryType.getCategory()));
            }

            boolean isIncluded = true;
            if(includedCategories.get() != null && includedCategories.get().size()>0)
            {
                isIncluded = includedCategories.get().stream().anyMatch(s -> s.equalsIgnoreCase(auditLogEntryType.getCategory()));
            }

            return isExcluded || (!isIncluded);
        }
        return false;
    }


}
