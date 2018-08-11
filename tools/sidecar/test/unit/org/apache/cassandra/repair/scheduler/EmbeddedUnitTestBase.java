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

package org.apache.cassandra.repair.scheduler;


import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerConfig;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerContext;
import org.apache.cassandra.repair.scheduler.conn.Cass4xInteraction;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.dao.cass.CassDaoUtil;
import org.cassandraunit.AbstractCassandraUnit4CQLTestCase;
import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;


public class EmbeddedUnitTestBase extends AbstractCassandraUnit4CQLTestCase
{
    protected static int jmxPort = 0;
    protected RepairSchedulerContext context = null;
    protected Set<String> sysKs = ImmutableSet.of("system_auth", "system_distributed", "system_schema");
    protected static final String REPAIR_SCHEDULER_KS_NAME = "system_distributed_test";


    @BeforeClass
    public static void setupJmxPort() throws IOException
    {
        if (jmxPort == 0)
        {
            ServerSocket s = new ServerSocket(0);
            jmxPort = s.getLocalPort();

            System.setProperty("cassandra.jmx.local.port", Integer.toString(jmxPort));
        }
    }

    @Override
    public CQLDataSet getDataSet()
    {
        return new ClassPathCQLDataSet("setupRepairSchedulerTables.cql", true, false, "system_distributed_test");
    }

    public CassDaoUtil getCassDaoUtil()
    {
        return new CassDaoUtil(getContext().getConfig(), this::getSession);
    }

    public Session getLocaldb()
    {
        return getSession();
    }

    public RepairSchedulerContext getContext()
    {
        if (context == null)
            context = new TestRepairSchedulerContext();
        return context;
    }

    protected List<String> getTables()
    {
        List<String> tables = new LinkedList<>();
        tables.add("repair_status");
        tables.add("repair_config");
        tables.add("repair_process");
        tables.add("repair_sequence");
        tables.add("repair_hook_status");
        tables.sort(String::compareTo);
        return tables;
    }

    protected class TestRepairSchedulerConfig extends RepairSchedulerConfig
    {
        public String getRepairKeyspace()
        {
            return "system_distributed_test";
        }

        @Override
        public int getLocalJmxPort()
        {
            return jmxPort;
        }

        @Override
        public String getRepairNativeEndpoint()
        {
            return "localhost:"+jmxPort;
        }

        @Override
        public List<String> getRepairStatePersistenceEndpoints()
        {
            InetSocketAddress addr = getSession().getCluster().getMetadata().getAllHosts().stream()
                                                 .findFirst().get().getSocketAddress();

            return Collections.singletonList(addr.getAddress().toString() + ":" + addr.getPort());
        }
    }

    protected class TestRepairSchedulerContext implements RepairSchedulerContext
    {
        protected RepairSchedulerConfig config;
        protected CassandraInteraction interaction;


        public Session localSession()
        {
            return getLocaldb();
        }

        public RepairSchedulerConfig getConfig()
        {
            if (this.config == null) this.config = new TestRepairSchedulerConfig();
            return this.config;
        }

        @Override
        public CassandraInteraction getCassInteraction()
        {
            if (this.interaction == null)
            {
                this.interaction = new Cass4xInteraction(getConfig());
            }
            return this.interaction;
        }
    }
}