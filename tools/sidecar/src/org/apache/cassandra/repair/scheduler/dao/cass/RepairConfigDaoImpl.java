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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerConfig;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerContext;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairConfigDao;
import org.apache.cassandra.repair.scheduler.entity.RepairOptions;
import org.apache.cassandra.repair.scheduler.entity.RepairType;
import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;

public class RepairConfigDaoImpl implements IRepairConfigDao
{
    private static final Logger logger = LoggerFactory.getLogger(RepairHookDaoImpl.class);

    private final RepairSchedulerConfig config;
    private final CassDaoUtil daoUtil;

    public RepairConfigDaoImpl(RepairSchedulerContext context, CassDaoUtil daoUtil)
    {
        this.daoUtil = daoUtil;
        this.config = context.getConfig();
    }

    /**
     * Get table repair configurations/ overrides for all schedules
     *
     * @return Map of TableRepairConfigs list keyed by schedule name
     */
    @Override
    public Map<String, List<TableRepairConfig>> getRepairConfigs(String clusterName)
    {
        Map<String, List<TableRepairConfig>> repairConfigListMap = new HashMap<>();


        Statement selectQuery = QueryBuilder.select()
                                            .from(config.getRepairKeyspace(), config.getRepairConfigTableName())
                                            .where(QueryBuilder.eq("cluster_name", clusterName));

        daoUtil.execSelectStmtRepairDb(selectQuery)
               .forEach(row ->
                        {
                            TableRepairConfig tableRepairConfig = getTableRepairConfig(row);
                            String schedule = row.getString("schedule_name");
                            repairConfigListMap.computeIfAbsent(schedule, k -> new ArrayList<>());
                            repairConfigListMap.get(schedule).add(tableRepairConfig);
                        });

        return repairConfigListMap;
    }

    @Override
    public List<TableRepairConfig> getRepairConfigs(String clusterName, String scheduleName)
    {

        List<TableRepairConfig> lstTableRepairConfig = new ArrayList<>();
        Statement selectQuery = QueryBuilder.select()
                                            .from(config.getRepairKeyspace(), config.getRepairConfigTableName())
                                            .where(QueryBuilder.eq("cluster_name", clusterName))
                                            .and(QueryBuilder.eq("schedule_name", scheduleName));

        daoUtil.execSelectStmtRepairDb(selectQuery).forEach(row -> {
            TableRepairConfig TableRepairConfig = getTableRepairConfig(row);
            lstTableRepairConfig.add(TableRepairConfig);
        });

        return lstTableRepairConfig;
    }

    private boolean hasColumn(Row row, String columnName)
    {
        return row.getColumnDefinitions().contains(columnName) && !row.isNull(columnName);
    }

    private TableRepairConfig getTableRepairConfig(Row row)
    {
        TableRepairConfig tableRepairConfig = new TableRepairConfig(config, row.getString("schedule_name"))
                                              .setKeyspace(row.getString("keyspace_name"))
                                              .setName(row.getString("table_name"));

        RepairOptions repairOptions = tableRepairConfig.getRepairOptions();

        repairOptions.setType(RepairType.fromString(row.getString("type")));

        if (hasColumn(row, "interrepair_delay_minutes"))
            tableRepairConfig.setInterRepairDelayMinutes(row.getInt("interrepair_delay_minutes"));

        if (hasColumn(row, "hooks"))
            tableRepairConfig.setPostRepairHooks(row.getList("hooks", String.class));

        if (hasColumn(row, "parallelism"))
            repairOptions.setParallelism(RepairParallelism.fromName(row.getString("parallelism")));

        if (hasColumn(row, "workers"))
            repairOptions.setNumWorkers(row.getInt("workers"));

        if (hasColumn(row, "split_strategy"))
            repairOptions.setSplitStrategy(row.getString("split_strategy"));

        // TODO: do we actually need to set this to 1?
        // In testing with concurrent sessions incremental would just get stuck a lot...
        if (repairOptions.getType() == RepairType.INCREMENTAL)
            repairOptions.setNumWorkers(1);

        if (hasColumn(row, "repair_timeout_seconds"))
            repairOptions.setSecondsToWait(row.getInt("repair_timeout_seconds"));

        tableRepairConfig.setRepairOptions(repairOptions);
        return tableRepairConfig;
    }

    @Override
    public Set<String> getRepairSchedules(String clusterName)
    {
        Set<String> schedules = new HashSet<>();
        Statement selectQuery = QueryBuilder.select("schedule_name")
                                            .from(config.getRepairKeyspace(), config.getRepairConfigTableName())
                                            .where(QueryBuilder.eq("cluster_name", clusterName));

        daoUtil.execSelectStmtRepairDb(selectQuery)
               .forEach(row -> schedules.add(row.getString("schedule_name")));

        //Get default schedules from config
        schedules.addAll(config.getDefaultSchedules());
        return schedules;
    }

    @Override
    public boolean saveRepairConfig(String clusterName, String schedule, TableRepairConfig repairConfig)
    {
        logger.info("Saving Repair Configuration for {}.{}", clusterName, schedule);
        try
        {
            Statement insertQuery = QueryBuilder.insertInto(config.getRepairKeyspace(), config.getRepairConfigTableName())
                                                .value("cluster_name", clusterName)
                                                .value("schedule_name", schedule)
                                                .value("keyspace_name", repairConfig.getKeyspace())
                                                .value("table_name", repairConfig.getName())
                                                .value("parallelism", repairConfig.getRepairOptions().getParallelism().toString())
                                                .value("type", repairConfig.getRepairOptions().getType().toString())
                                                .value("interrepair_delay_minutes", repairConfig.getInterRepairDelayMinutes())
                                                .value("workers", repairConfig.getRepairOptions().getNumWorkers())
                                                .value("split_strategy", repairConfig.getRepairOptions().getSplitStrategy().toString())
                                                .value("hooks", repairConfig.getPostRepairHooks());

            daoUtil.execUpsertStmtRepairDb(insertQuery);
        }
        catch (Exception e)
        {
            logger.error("Exception in marking cluster post repair hook status started", e);
            return false;
        }
        return true;
    }
}
