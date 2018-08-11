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
package org.apache.cassandra.repair.scheduler.entity;

import java.net.InetAddress;
import java.util.Date;
import java.util.Optional;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Token;

public class RepairHost
{
    private InetAddress broadcastAddress;
    private String datacenter;
    private String rack;
    private String nodeId;
    private Date lastRepairTime;
    private Token firstToken;

    public RepairHost(InetAddress broadcastAddress, String datacenter, String rack, String nodeId)
    {
        this.broadcastAddress = broadcastAddress;
        this.datacenter = datacenter;
        this.rack = rack;
        this.nodeId = nodeId;
    }

    // From Datastax driver host
    public RepairHost(Host host)
    {
        this.broadcastAddress = host.getBroadcastAddress();
        this.datacenter = host.getDatacenter();
        this.rack = host.getRack();

        Optional<Token> firstToken = host.getTokens().stream().findFirst();
        firstToken.ifPresent(token -> this.firstToken = token);
    }

    public InetAddress getBroadcastAddress()
    {
        return broadcastAddress;
    }

    public RepairHost setBroadcastAddress(InetAddress broadcastAddress)
    {
        this.broadcastAddress = broadcastAddress;
        return this;
    }

    public String getDatacenter()
    {
        return datacenter;
    }

    public RepairHost setDatacenter(String datacenter)
    {
        this.datacenter = datacenter;
        return this;
    }

    public String getRack()
    {
        return rack;
    }

    public RepairHost setRack(String rack)
    {
        this.rack = rack;
        return this;
    }

    public String getNodeId()
    {
        return nodeId;
    }

    public RepairHost setNodeId(String nodeId)
    {
        this.nodeId = nodeId;
        return this;
    }

    public Optional<Date> getLastRepairTime()
    {
        return Optional.ofNullable(lastRepairTime);
    }

    public RepairHost setLastRepairTime(Date lastRepairTime)
    {
        this.lastRepairTime = lastRepairTime;
        return this;
    }

    public Token getFirstToken()
    {
        return firstToken;
    }

    public RepairHost setFirstToken(Token firstToken)
    {
        this.firstToken = firstToken;
        return this;
    }
}
