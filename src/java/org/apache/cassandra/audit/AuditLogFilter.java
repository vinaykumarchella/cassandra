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

    private final ImmutableSet<String> excludedKeyspaces;
    private final ImmutableSet<String> includedKeyspaces;
    private final ImmutableSet<String> excludedCategories;
    private final ImmutableSet<String> includedCategories;
    private final ImmutableSet<String> includedUsers;
    private final ImmutableSet<String> excludedUsers;

    private AuditLogFilter(ImmutableSet<String> excludedKeyspaces, ImmutableSet<String> includedKeyspaces, ImmutableSet<String> excludedCategories, ImmutableSet<String> includedCategories, ImmutableSet<String> excludedUsers, ImmutableSet<String> includedUsers)
    {
        this.excludedKeyspaces = excludedKeyspaces;
        this.includedKeyspaces = includedKeyspaces;
        this.excludedCategories = excludedCategories;
        this.includedCategories = includedCategories;
        this.includedUsers = includedUsers;
        this.excludedUsers = excludedUsers;
    }

    /**
     * (Re-)Loads filters from config. Called during startup as well as JMX filter reload.
     */
    public static AuditLogFilter create()
    {
        logger.trace("Loading AuditLog filters");
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


        return new AuditLogFilter(ImmutableSet.copyOf(excludedKeyspacesSet), ImmutableSet.copyOf(includedKeyspacesSet),
                                  ImmutableSet.copyOf(excludedCategoriesSet), ImmutableSet.copyOf(includedCategoriesSet),
                                  ImmutableSet.copyOf(excludedUsersSet), ImmutableSet.copyOf(includedUsersSet));
    }

    /**
     * Constructs mutually exclusive sets of included and excluded data. When there is a conflict,
     * the entry is put into the excluded set (and removed fron the included).
     */
    private static void loadInputSets(Set<String> includedSet, String includedInput, Set<String> excludedSet, String excludedInput)
    {
        for (String exclude : excludedInput.split(","))
        {
            if (!exclude.isEmpty())
            {
                excludedSet.add(exclude);
            }
        }

        for (String include : includedInput.split(","))
        {
            //Ensure both included and excluded sets are mutually exclusive
            if (!include.isEmpty() && !excludedSet.contains(include))
            {
                includedSet.add(include);
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
     * If excludeSet does not contain any items, by default nothing is excluded (unless there are
     * entries in the includeSet).
     * If includeSet does not contain any items, by default everything is included
     * If an input is part of both includeSet and excludeSet, excludeSet takes the priority over includeSet
     *
     * @param input      Input to be checked for filtereing based on includeSet and excludeSet
     * @param includeSet Include filtering set
     * @param excludeSet Exclude filtering set
     * @return true if the input is filtered, false when the input is not filtered
     */
    static boolean isFiltered(String input, Set<String> includeSet, Set<String> excludeSet)
    {
        if (!excludeSet.isEmpty() && excludeSet.contains(input))
            return true;

        return !(includeSet.isEmpty() || includeSet.contains(input));
    }
}
