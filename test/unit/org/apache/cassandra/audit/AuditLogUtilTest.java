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

import org.junit.Assert;
import org.junit.Test;


public class AuditLogUtilTest
{

    @Test
    public void isFiltered_IncludeSetOnly() throws Exception
    {
        Set<String> includeSet = new HashSet<>();
        includeSet.add("a");
        includeSet.add("b");
        includeSet.add("c");

        Set<String> excludeSet = new HashSet<>();


        Assert.assertFalse(AuditLogUtil.isFiltered("a", includeSet, excludeSet));

        Assert.assertFalse(AuditLogUtil.isFiltered("b", includeSet, excludeSet));
        Assert.assertFalse(AuditLogUtil.isFiltered("c", includeSet, excludeSet));

        Assert.assertTrue(AuditLogUtil.isFiltered("d", includeSet, excludeSet));
        Assert.assertTrue(AuditLogUtil.isFiltered("e", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_ExcludeSetOnly() throws Exception
    {
        Set<String> includeSet = new HashSet<>();

        Set<String> excludeSet = new HashSet<>();
        excludeSet.add("a");
        excludeSet.add("b");
        excludeSet.add("c");


        Assert.assertTrue(AuditLogUtil.isFiltered("a", includeSet, excludeSet));

        Assert.assertTrue(AuditLogUtil.isFiltered("b", includeSet, excludeSet));
        Assert.assertTrue(AuditLogUtil.isFiltered("c", includeSet, excludeSet));

        Assert.assertFalse(AuditLogUtil.isFiltered("d", includeSet, excludeSet));
        Assert.assertFalse(AuditLogUtil.isFiltered("e", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_MutualExclusive() throws Exception
    {
        Set<String> includeSet = new HashSet<>();
        includeSet.add("a");
        includeSet.add("b");
        includeSet.add("c");

        Set<String> excludeSet = new HashSet<>();
        excludeSet.add("a");

        Assert.assertTrue(AuditLogUtil.isFiltered("a", includeSet, excludeSet));
        Assert.assertFalse(AuditLogUtil.isFiltered("b", includeSet, excludeSet));
        Assert.assertFalse(AuditLogUtil.isFiltered("c", includeSet, excludeSet));

        Assert.assertTrue(AuditLogUtil.isFiltered("e", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_MutualInclusive() throws Exception
    {
        Set<String> includeSet = new HashSet<>();
        includeSet.add("a");
        includeSet.add("b");

        Set<String> excludeSet = new HashSet<>();
        excludeSet.add("c");
        excludeSet.add("d");

        Assert.assertFalse(AuditLogUtil.isFiltered("a", includeSet, excludeSet));
        Assert.assertFalse(AuditLogUtil.isFiltered("b", includeSet, excludeSet));

        Assert.assertTrue(AuditLogUtil.isFiltered("c", includeSet, excludeSet));
        Assert.assertTrue(AuditLogUtil.isFiltered("d", includeSet, excludeSet));

        Assert.assertTrue(AuditLogUtil.isFiltered("e", includeSet, excludeSet));
        Assert.assertTrue(AuditLogUtil.isFiltered("f", includeSet, excludeSet));

    }
    @Test
    public void isFiltered_UnSpecifiedInput() throws Exception
    {
        Set<String> includeSet = new HashSet<>();
        includeSet.add("a");
        includeSet.add("b");
        includeSet.add("c");

        Set<String> excludeSet = new HashSet<>();
        excludeSet.add("a");

        Assert.assertTrue(AuditLogUtil.isFiltered("a", includeSet, excludeSet));

        Assert.assertFalse(AuditLogUtil.isFiltered("b", includeSet, excludeSet));
        Assert.assertFalse(AuditLogUtil.isFiltered("c", includeSet, excludeSet));

        Assert.assertTrue(AuditLogUtil.isFiltered("d", includeSet, excludeSet));
        Assert.assertTrue(AuditLogUtil.isFiltered("e", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_SpecifiedInput() throws Exception
    {
        Set<String> includeSet = new HashSet<>();
        includeSet.add("a");
        includeSet.add("b");
        includeSet.add("c");

        Set<String> excludeSet = new HashSet<>();
        excludeSet.add("a");

        Assert.assertTrue(AuditLogUtil.isFiltered("a", includeSet, excludeSet));

        Assert.assertFalse(AuditLogUtil.isFiltered("b", includeSet, excludeSet));
        Assert.assertFalse(AuditLogUtil.isFiltered("c", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_FilteredInput_EmptyInclude() throws Exception
    {
        Set<String> includeSet = new HashSet<>();


        Set<String> excludeSet = new HashSet<>();
        excludeSet.add("a");

        Assert.assertTrue(AuditLogUtil.isFiltered("a", includeSet, excludeSet));
        Assert.assertFalse(AuditLogUtil.isFiltered("b", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_FilteredInput_EmptyExclude() throws Exception
    {
        Set<String> includeSet = new HashSet<>();
        includeSet.add("a");
        includeSet.add("b");
        includeSet.add("c");

        Set<String> excludeSet = new HashSet<>();

        Assert.assertFalse(AuditLogUtil.isFiltered("a", includeSet, excludeSet));
        Assert.assertFalse(AuditLogUtil.isFiltered("b", includeSet, excludeSet));
        Assert.assertFalse(AuditLogUtil.isFiltered("c", includeSet, excludeSet));

        Assert.assertTrue(AuditLogUtil.isFiltered("e", includeSet, excludeSet));

    }

    @Test
    public void isFiltered_EmptyInputs() throws Exception
    {
        Set<String> includeSet = new HashSet<>();

        Set<String> excludeSet = new HashSet<>();

        Assert.assertFalse(AuditLogUtil.isFiltered("a", includeSet, excludeSet));

        Assert.assertFalse(AuditLogUtil.isFiltered("e", includeSet, excludeSet));

    }
}