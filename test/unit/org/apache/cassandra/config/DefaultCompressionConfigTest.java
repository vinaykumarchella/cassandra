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
package org.apache.cassandra.config;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static org.junit.Assert.assertEquals;

public class DefaultCompressionConfigTest extends SchemaLoader
{
    private static final String KS_NAME = "default_compression_test";
    private static final String CF_NAME = "test_cf";


    private static ColumnFamilyStore BACKEND;

    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        System.setProperty("cassandra.config", "cassandra_default_compression.yaml");
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KS_NAME, KeyspaceParams.simpleTransient(1));

    }

    @Test
    public void testDefaultCompression_None() throws Exception
    {

        EmbeddedCassandraService cassandra;

        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        Session session = cluster.connect();

        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int, val1 text, val2 text, PRIMARY KEY(id, val1));", KS_NAME, CF_NAME));

        BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);
        assertEquals("false",BACKEND.getCompressionParameters().get("enabled"));


    }
}
