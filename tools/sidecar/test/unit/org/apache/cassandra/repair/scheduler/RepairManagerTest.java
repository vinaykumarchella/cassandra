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

import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.scheduler.entity.RepairOptions;
import org.apache.cassandra.repair.scheduler.entity.RepairType;
import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;

import org.junit.Before;
import org.junit.Test;

public class RepairManagerTest extends EmbeddedUnitTestBase
{
    @Before
    public void setupManager() throws InterruptedException
    {
        loadDataset(30000);
        RepairDaoManager daoManager = new RepairDaoManager(getContext());

        TableRepairConfig testConfig = new TableRepairConfig(getContext().getConfig(), context.getConfig().getDefaultSchedule());
        RepairOptions options = testConfig.getRepairOptions();

        // Full + subrange table
        options.setNumWorkers(2)
               .setSplitStrategy("1000")
               .setType(RepairType.fromString("full"))
               .setParallelism(RepairParallelism.fromName("sequential"));

        testConfig.setKeyspace("test_repair")
                  .setName("subrange_test")
                  .setRepairOptions(options)
                  .setInterRepairDelayMinutes(10);

        daoManager.getRepairConfigDao().saveRepairConfig("default", testConfig);

        // Incremental table
        options.setNumWorkers(2)
               .setSplitStrategy("100")
               .setType(RepairType.fromString("incremental"))
               .setParallelism(RepairParallelism.fromName("sequential"));

        testConfig.setKeyspace("test_repair")
                  .setName("incremental_test")
                  .setRepairOptions(options)
                  .setInterRepairDelayMinutes(10);

        daoManager.getRepairConfigDao().saveRepairConfig("default", testConfig);

        // Disabled table is inserted directly into the repair config to simulate someone supplying
        // _just_ the disabled type.
        getContext().localSession().execute(
        "INSERT INTO system_distributed.repair_config (cluster_name, schedule_name, keyspace_name, table_name, type) " +
        "VALUES ('" + context.getCassInteraction().getClusterName() + "', 'default', 'test_repair', 'no_repair', 'disabled')");
    }

    @Test
    public void testRunRepairOnCluster()
    {
        RepairManager rm = new RepairManager(getContext());
        int repairId = rm.runRepairOnCluster();
        System.out.println(repairId);
    }
}
