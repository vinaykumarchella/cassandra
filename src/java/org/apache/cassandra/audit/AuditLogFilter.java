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

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

public class AuditLogFilter
{
    private static final Logger logger = LoggerFactory.getLogger(AuditLogFilter.class);

    private volatile ImmutableSet<String> excludedKeyspaces = ImmutableSet.of();
    private volatile ImmutableSet<String> includedKeyspaces = ImmutableSet.of();

    private volatile ImmutableSet<String> excludedCategories = ImmutableSet.of();
    private volatile ImmutableSet<String> includedCategories = ImmutableSet.of();

    private volatile ImmutableSet<String> includedUsers = ImmutableSet.of();
    private volatile ImmutableSet<String> excludedUsers = ImmutableSet.of();

    private static final AuditLogFilter instance = new AuditLogFilter();

    private AuditLogFilter()
    {
        loadFilters();
    }

    public static AuditLogFilter getInstance()
    {
        return instance;
    }

    /**
     * (Re)Loads filters from config. This is being called during startup as well as JMX filter reload
     */
    public final void loadFilters()
    {
        logger.info("Loading AuditLog filters");
        Set<String> includedKeyspacesSet = new HashSet<>();
        Set<String> excludedKeyspacesSet = new HashSet<>();

        Set<String> excludedCategoriesSet = new HashSet<>();
        Set<String> includedCategoriesSet = new HashSet<>();


        Set<String> excludedUsersSet = new HashSet<>();
        Set<String> includedUsersSet = new HashSet<>();

        loadInputSets(includedKeyspacesSet, DatabaseDescriptor.getAuditLoggingOptions().included_keyspaces,
                      excludedKeyspacesSet, DatabaseDescriptor.getAuditLoggingOptions().excluded_keyspaces);

        loadInputSets(includedCategoriesSet, DatabaseDescriptor.getAuditLoggingOptions().included_categories,
                      excludedCategoriesSet, DatabaseDescriptor.getAuditLoggingOptions().excluded_categories);

        loadInputSets(includedUsersSet, DatabaseDescriptor.getAuditLoggingOptions().included_users,
                      excludedUsersSet, DatabaseDescriptor.getAuditLoggingOptions().excluded_users);


        includedKeyspaces = ImmutableSet.copyOf(includedKeyspacesSet);
        excludedKeyspaces = ImmutableSet.copyOf(excludedKeyspacesSet);

        includedCategories = ImmutableSet.copyOf(includedCategoriesSet);
        excludedCategories = ImmutableSet.copyOf(excludedCategoriesSet);

        includedUsers = ImmutableSet.copyOf(includedUsersSet);
        excludedUsers = ImmutableSet.copyOf(excludedUsersSet);
    }

    /**
     * Constructs mutually exclusive sets with excluded set being the default option when there are conlicting inputs
     */
    private void loadInputSets(Set<String> includedSet, String includedInput, Set<String> excludedSet, String excludedInput)
    {
        for (String keyspace : excludedInput.split(","))
        {
            if (!keyspace.isEmpty())
            {
                excludedSet.add(keyspace);
            }
        }
        for (String keyspace : includedInput.split(","))
        {
            //Ensure both included and excluded sets are mutually exclusive
            if (!keyspace.isEmpty() && !excludedSet.contains(keyspace))
            {
                includedSet.add(keyspace);
            }
        }
    }

    /**
     * Checks whether a give AuditLog Entry is filtered or not
     *
     * @param auditLogEntry AuditLogEntry to verify
     * @return true if it is filtered, false otherwise
     */
    boolean isFiltered(AuditLogEntry auditLogEntry)
    {
        return isFiltered(auditLogEntry.getKeyspace(), includedKeyspaces, excludedKeyspaces)
               || isFiltered(auditLogEntry.getType().getCategory(), includedCategories, excludedCategories)
               || isFiltered(auditLogEntry.getUser(), includedUsers, excludedUsers);
    }

    /**
     * Checks whether given input is being filtered or not.
     * If includeSet does not contain any items, by default everything is included
     * If excludeSet does not contain any items, by default nothing is excluded.
     * If an input is part of both includeSet and excludeSet, excludeSet takes the priority over includeSet
     *
     * @param input      Input to be checked for filtereing based on includeSet and excludeSet
     * @param includeSet Include filtering set
     * @param excludeSet Exclude filtering set
     * @return true if the input is filtered, false when the input is not filtered
     */
    private boolean isFiltered(String input, Set<String> includeSet, Set<String> excludeSet)
    {
        if (input != null && !input.isEmpty())
        {
            if (excludeSet.size() > 0)
            {
                return excludeSet.contains(input);
            }
            else if (includeSet.size() > 0)
            {
                return !includeSet.contains(input);
            }
        }
        return false;
    }
}
