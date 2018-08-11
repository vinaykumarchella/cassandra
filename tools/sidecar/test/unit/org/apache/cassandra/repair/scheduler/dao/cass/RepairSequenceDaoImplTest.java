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

import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.repair.scheduler.dao.model.IRepairSequenceDao;
import org.apache.cassandra.repair.scheduler.entity.ClusterRepairStatus;
import org.apache.cassandra.repair.scheduler.entity.RepairSequence;
import org.apache.cassandra.repair.scheduler.entity.RepairStatus;
import org.joda.time.DateTimeUtils;

public class RepairSequenceDaoImplTest extends BaseDaoUnitTest
{
    public IRepairSequenceDao repairSequenceDao;

    @Before
    public void beforeMethod()
    {
        context = getContext();
        repairSequenceDao = new RepairSequenceDaoImpl(context, getCassDaoUtil());
    }


    @After
    public void cleanupMethod()
    {
        ClusterRepairStatus status = repairSequenceDao.getLatestRepairId();

        if (status.getRepairId() > 0)
        {
            //TODO: Implement deleteEndpointSeqMap
            // repairSequenceDao.deleteEndpointSeqMap(status.getRepairId());
        }
    }

    @Before
    public void setUpMethod()
    {
        repairId = getRandomRepairId();
    }

    @Test
    public void getLatestRepairIdNotExists() throws Exception
    {
        ClusterRepairStatus newRepairStatus = repairSequenceDao.getLatestRepairId();
        Assert.assertEquals(-1, newRepairStatus.getRepairId());
    }

    @Test
    public void getLatestRepairIdExists() throws Exception
    {
        repairSequenceDao.markRepairStartedOnInstance(1, 1);
        ClusterRepairStatus status = repairSequenceDao.getLatestRepairId();
        Assert.assertEquals(1, status.getRepairId());

    }

    @Test
    //@TestPropertyOverride(value = {"nfcassrepairservice.getConfig.keyspaceName=cassrepair"})
    public void markRepairStartedOnInstance() throws Exception
    {
        Assert.assertTrue(repairSequenceDao.markRepairStartedOnInstance(repairId, 222));
        SortedSet<RepairSequence> rSeq = repairSequenceDao.getRepairSequence(repairId);
        Assert.assertEquals(repairId, rSeq.first().getRepairId());
        Assert.assertEquals(Optional.of(222), Optional.of(rSeq.first().getSeq()));
        Assert.assertEquals(RepairStatus.STARTED, rSeq.first().getStatus());
    }

    @Test
    public void markRepairCompletedOnInstance() throws Exception
    {

    }

    @Test
    public void persistEndpointSeqMap() throws Exception
    {
        repairSequenceDao.persistEndpointSeqMap(repairId, "default", generateHosts(3));
        Assert.assertEquals(3, repairSequenceDao.getRepairSequence(repairId).size());
    }

    @Test
    public void getRepairSequence() throws Exception
    {
        Map<String, String> endpointMap = generateEndpointToHostId(3);
        repairSequenceDao.persistEndpointSeqMap(repairId, "default", generateHosts(3));
        SortedSet<RepairSequence> rSeq = repairSequenceDao.getRepairSequence(repairId);
        Assert.assertEquals(3, rSeq.size());
    }

    @Test
    public void updateHeartBeat() throws Exception
    {

    }

    @Test
    public void getLatestInProgressHeartBeat() throws Exception
    {
        repairSequenceDao.markRepairStartedOnInstance(repairId, 1);
        ClusterRepairStatus status = repairSequenceDao.getLatestRepairId();

        Assert.assertEquals(repairId, status.getRepairId());

        Optional<RepairSequence> rSeq = repairSequenceDao.getStuckRepairSequence(repairId);

        Assert.assertFalse(rSeq.isPresent());
        repairSequenceDao.updateHeartBeat(repairId, 1);
        DateTimeUtils.setCurrentMillisOffset(31 * 60 * 1000);
        Optional<RepairSequence> rSeq2 = repairSequenceDao.getStuckRepairSequence(repairId);
        Assert.assertTrue(rSeq2.isPresent());
        Assert.assertEquals("Ok", rSeq2.get().getLastEvent().get("Status"));

    }

    @Test
    public void cancelRepairOnNode() throws Exception
    {
        int repairId = getRandomRepairId();
        boolean rSeq = repairSequenceDao.cancelRepairOnNode(repairId, 2);

        SortedSet<RepairSequence> r = repairSequenceDao.getRepairSequence(repairId);
        Assert.assertEquals(1, r.size());
        Assert.assertEquals(RepairStatus.CANCELLED, r.first().getStatus());

    }


}