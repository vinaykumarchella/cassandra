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

import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;

public class AuditLogFilter
{
    private List<String> excludedKeyspaces = new LinkedList<>(), includedKeyspaces = new LinkedList<>();

    private static final AuditLogFilter instance = new AuditLogFilter();

    public AuditLogFilter()
    {

    }

    public static AuditLogFilter getInstance()
    {
     return instance;
    }
    public void loadFilters()
    {
        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().includedKeyspaces.split(","))
        {
            if(!keyspace.isEmpty())
            {
                includedKeyspaces.add(keyspace);
            }
        }
        for (String keyspace : DatabaseDescriptor.getAuditLoggingOptions().excludedKeyspaces.split(","))
        {
            if(!keyspace.isEmpty())
            {
                excludedKeyspaces.add(keyspace);
            }
        }

    }
    public void reloadFilters()
    {
        excludedKeyspaces = new LinkedList<>();
        includedKeyspaces = new LinkedList<>();
        this.loadFilters();
    }

    public boolean isFiltered(String keyspace)
    {
        if(keyspace != null && !keyspace.isEmpty())
        {
            boolean isExcluded = false;
            if(excludedKeyspaces != null && excludedKeyspaces.size()>0)
            {
                isExcluded = excludedKeyspaces.stream().anyMatch(s -> s.equalsIgnoreCase(keyspace));
            }

            boolean isIncluded = true;
            if(includedKeyspaces != null && includedKeyspaces.size()>0)
            {
                isIncluded = includedKeyspaces.stream().anyMatch(s -> s.equalsIgnoreCase(keyspace));
            }

            return isExcluded || (!isIncluded);
        }
        return false;
    }
}
