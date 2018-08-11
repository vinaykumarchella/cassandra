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

import java.util.Optional;

import org.apache.cassandra.repair.scheduler.entity.ClusterRepairStatus;

public interface IRepairProcessDao
{
    /**
     * Gets the latest repair status on the cluster.
     * This gives the repair status of entire cluster, repair not necessarily need to run on the current instance
     *
     * @return Cluster repair status information from repair_process table, this does contain node level repair status
     */
    Optional<ClusterRepairStatus> getClusterRepairStatus();

    Optional<ClusterRepairStatus> getClusterRepairStatus(int repairId);

    boolean acquireRepairInitLock(int repairId);

    boolean markClusterRepairFinished(int repairId);

    boolean deleteClusterRepairStatus(int repairId);

    boolean updateClusterRepairStatus(ClusterRepairStatus clusterRepairStatus);
}
