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
package org.apache.cassandra.repair.scheduler.dao.cass;

import org.apache.cassandra.repair.scheduler.EmbeddedUnitTestBase;
import org.apache.cassandra.repair.scheduler.entity.RepairHost;
import org.apache.cassandra.repair.scheduler.entity.RepairMetadata;
import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;
import org.apache.cassandra.utils.GuidGenerator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class BaseDaoUnitTest extends EmbeddedUnitTestBase
{
    public static final String TEST_CLUSTER_NAME = "Test Cluster";
    private static final int MASK = (-1) >>> 1; // all ones except the sign bit
    int repairId = getRandomRepairId();//-1;

    List<RepairHost> generateHosts(int n)
    {
        List<RepairHost> allHosts = new LinkedList<>();
        for (int i = 0; i < n; i++)
        {
            try
            {
                RepairHost host = new RepairHost(InetAddress.getByName("127.0.0." + i), "dc1", "rack1", GuidGenerator.guid());
                allHosts.add(host);
            } catch (UnknownHostException e) { }
        }
        return allHosts;
    }

    Map<String, String> generateEndpointToHostId(int n)
    {
        return generateEndpointToHostId(generateHosts(n));
    }

    Map<String, String> generateEndpointToHostId(List<RepairHost> hosts)
    {
        return hosts.stream().collect(Collectors.toMap(n -> n.getBroadcastAddress().getHostName(), RepairHost::getNodeId));
    }

    int getRandomRepairId()
    {
        return new Random().nextInt() & MASK;
    }

    int nextRandomPositiveInt()
    {
        return new Random().nextInt() & MASK;
    }

    RepairMetadata generateRepairMetadata(String hostId)
    {
        RepairMetadata repairMetadata = new RepairMetadata();
        String testKS = "TestKS_" + nextRandomPositiveInt();
        String testTable = "TestTable_" + nextRandomPositiveInt();

        TableRepairConfig repairConfig = new TableRepairConfig(getContext().getConfig(), "default");
        repairConfig.setKeyspace(testKS).setName(testTable);

        repairMetadata.setClusterName(TEST_CLUSTER_NAME)
                      .setRepairId(repairId)
                      .setNodeId(hostId)
                      .setKeyspaceName(testKS)
                      .setTableName(testTable)
                      .setRepairNum(nextRandomPositiveInt())
                      .setStartToken("STARTToken_" + nextRandomPositiveInt())
                      .setEndToken("ENDToken_" + nextRandomPositiveInt())
                      .setRepairConfig(repairConfig);


        return repairMetadata;
    }

}
