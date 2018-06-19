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
package org.apache.cassandra.cql3.validation.miscellaneous;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.CircuitBreakerRowLimitException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

/**
 * Test that various CircuitBreaker Exceptions are thrown when they should be and doesn't when they shouldn't be.
 * Currently tests:
 *  * CircuitBreakerRowLimitException (range slices without pagination that consume all memory)
 */
public class CircuitBreakerTest extends CQLTester
{
    static final int ORIGINAL_THRESHOLD = DatabaseDescriptor.getCircuitBreakerRowFilterLimit();
    static final int THRESHOLD = 100;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setCircuitBreakerRowFilterLimit(THRESHOLD);
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setCircuitBreakerRowFilterLimit(ORIGINAL_THRESHOLD);
    }

    @Test
    public void testBelowThresholdSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");

        // insert exactly the amount of rows that shouldn't trigger an exception
        for (int i = 0; i < THRESHOLD; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key" + i + "', 'column', 'test');");

        execute("SELECT * FROM %s");
    }

    @Test(expected = CircuitBreakerRowLimitException.class)
    public void testAboveThresholdSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");

        // insert exactly the number of rows that should trigger an exception
        for (int i = 0; i < THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key" + i + "', 'column', 'test');");

        execute("SELECT * FROM %s");
    }

    @Test
    public void testPaginationFixesAboveThresholdSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");

        // insert exactly the amount of rows that should trigger an exception
        Set<String> expectedResults = new HashSet<>();

        for (int i = 0; i < THRESHOLD + 1; i++)
        {
            String key = "key" + i;
            execute("INSERT INTO %s (a, b, c) VALUES ('" + key + "', 'column', 'test');");
            expectedResults.add(key);
        }

        // Must fail without pagination
        try
        {
            execute("SELECT * FROM %s");
            fail("SELECT with no pagination beyond the threshold should have failed, but hasn't");
        }
        catch (Throwable e)
        {
            assertTrue(e instanceof CircuitBreakerRowLimitException);
        }

        List<UntypedResultSet.Row> rsList = new ArrayList<>();

        // Should fail with paging > THRESHOLD
        try
        {
            UntypedResultSet rs = executeWithPaging("SELECT * FROM %s", THRESHOLD + 1);
            for (UntypedResultSet.Row r : rs)
            {
                rsList.add(r);
            }
            fail("SELECT with no pagination beyond the threshold should have failed, but hasn't");
        }
        catch (Throwable e)
        {
            assertTrue(e instanceof CircuitBreakerRowLimitException);
        }

        rsList.clear();

        // Must succeed with all data returned once paginated
        UntypedResultSet rs = executeWithPaging("SELECT * FROM %s", 20);
        for (UntypedResultSet.Row r : rs)
        {
            rsList.add(r);
        }
        assertEquals(rsList.size(), 101);
        Set<String> actualResults = new HashSet<>();
        for (UntypedResultSet.Row row : rsList)
        {
            actualResults.add(row.getString("a"));
            assertEquals(row.getString("b"), "column");
            assertEquals(row.getString("c"), "test");
        }
        assertTrue(actualResults.equals(expectedResults));
    }
}
