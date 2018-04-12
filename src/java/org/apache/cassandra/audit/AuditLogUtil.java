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

import java.net.InetSocketAddress;
import java.util.Set;


public class AuditLogUtil
{
    public static final InetSocketAddress DEFAULT_SOURCE =new InetSocketAddress("0.0.0.0",0);

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
    public static boolean isFiltered(String input, Set<String> includeSet, Set<String> excludeSet)
    {
        boolean isExcluded = false;
        if (excludeSet.size() > 0)
        {
            isExcluded = excludeSet.contains(input);
        }
        if (isExcluded)
        {
            return true;
        }
        else
        {
            boolean isIncluded = true;
            if (includeSet.size() > 0)
            {
                isIncluded = includeSet.contains(input);
            }
            return !isIncluded;
        }
    }
}
