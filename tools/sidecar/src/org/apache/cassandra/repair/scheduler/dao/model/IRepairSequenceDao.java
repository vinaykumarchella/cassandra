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

package org.apache.cassandra.repair.scheduler.dao.model;

import java.util.List;
import java.util.Optional;
import java.util.SortedSet;

import org.apache.cassandra.repair.scheduler.entity.ClusterRepairStatus;
import org.apache.cassandra.repair.scheduler.entity.RepairHost;
import org.apache.cassandra.repair.scheduler.entity.RepairSequence;

public interface IRepairSequenceDao
{
    ClusterRepairStatus getLatestRepairId();

    boolean markRepairStartedOnInstance(int repairId, int seq);

    boolean markRepairFinishedOnNode(int repairId, int seq);

    /**
     * Get the node sequence for a give Repair Id. Sorted in ascending order
     *
     * @param repairId
     * @return
     */
    Optional<RepairSequence> getMyNodeStatus(int repairId);

    void persistEndpointSeqMap(int repairId, String scheduleName, List<RepairHost> allHosts);

    SortedSet<RepairSequence> getRepairSequence(int repairId);

    boolean updateHeartBeat(int repairId, int seq);

    Optional<RepairSequence> getStuckRepairSequence(int repairId);

    boolean cancelRepairOnNode(int repairId, Integer seq);
}
