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

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.repair.scheduler.RepairDaoManager;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairSequenceDao;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairStatusDao;
import org.apache.cassandra.repair.scheduler.entity.RepairMetadata;
import org.apache.cassandra.repair.scheduler.entity.RepairStatus;

public class RepairStatusDaoImplTest extends BaseDaoUnitTest
{
    private IRepairStatusDao repairStatusDao;
    private String hostId;

    @Before
    public void beforeMethod()
    {
        context = getContext();
        repairStatusDao = new RepairStatusDaoImpl(context, getCassDaoUtil());
        IRepairSequenceDao repairSequenceDao = new RepairSequenceDaoImpl(context, getCassDaoUtil());
        hostId = context.getCassInteraction().getLocalHostId();
    }

    @After
    public void cleanupMethod()
    {
        context.localSession().execute("TRUNCATE TABLE "+context.getConfig().getRepairKeyspace()+"."+context.getConfig().repair_status_tablename+";");
    }

    @Before
    public void setUpMethod()
    {
        repairId = getRandomRepairId();
    }

    @Test
    public void markRepairStarted()
    {
        RepairMetadata repairMetadata = generateRepairMetadata(hostId).setStatus("STARTED");
        repairStatusDao.markRepairStatusChange(repairMetadata);
        List<RepairMetadata> result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(RepairStatus.STARTED, result.get(0).getStatus());
    }

    @Test
    public void markRepairPaused()
    {
        RepairMetadata repairMetadata = generateRepairMetadata(hostId).setStatus("PAUSED");
        repairStatusDao.markRepairStatusChange(repairMetadata);
        List<RepairMetadata> result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(RepairStatus.PAUSED, result.get(0).getStatus());
    }

    @Test
    public void markRepairFailed()
    {
        RepairMetadata repairMetadata = generateRepairMetadata(hostId).setStatus("FAILED");

        repairStatusDao.markRepairStatusChange(repairMetadata);
        List<RepairMetadata> result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(RepairStatus.FAILED, result.get(0).getStatus());
    }

    @Test
    public void markRepairCompleted()
    {
        RepairMetadata repairMetadata = generateRepairMetadata(hostId).setStatus("FINISHED");

        repairStatusDao.markRepairStatusChange(repairMetadata);
        List<RepairMetadata> result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(RepairStatus.FINISHED, result.get(0).getStatus());
    }

    @Test
    public void markRepairCancelled_NOTSTARTED()
    {
        RepairMetadata repairMetadata = generateRepairMetadata(hostId);

        repairStatusDao.markRepairCancelled(repairMetadata.getRepairId(), hostId);
        List<RepairMetadata> result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void markRepairCancelled_STARTED()
    {
        RepairMetadata repairMetadata = generateRepairMetadata(hostId).setStatus("STARTED");
        repairStatusDao.markRepairStatusChange(repairMetadata);

        List<RepairMetadata> result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(RepairStatus.STARTED, result.get(0).getStatus());
        Assert.assertEquals(hostId, result.get(0).getNodeId());

        repairStatusDao.markRepairCancelled(repairMetadata.getRepairId(), hostId);
        result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(RepairStatus.CANCELLED, result.get(0).getStatus());
    }

    @Test
    public void markRepairCancelled_STARTED_Multi()
    {
        RepairMetadata repairMetadata = generateRepairMetadata(hostId).setStatus("STARTED");
        repairStatusDao.markRepairStatusChange(repairMetadata);

        for (int i = 0; i < 4; i++)
        {
            repairMetadata = generateRepairMetadata(hostId).setStatus("STARTED");
            repairStatusDao.markRepairStatusChange(repairMetadata);
        }

        List<RepairMetadata> result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(RepairStatus.STARTED, result.get(0).getStatus());

        repairStatusDao.markRepairCancelled(repairMetadata.getRepairId(), hostId);
        result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(RepairStatus.CANCELLED, result.get(3).getStatus());
    }
}