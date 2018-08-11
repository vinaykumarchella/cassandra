package org.apache.cassandra.repair.scheduler.dao.cass;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerContext;
import org.apache.cassandra.repair.scheduler.dao.model.ILocalCassDao;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairConfigDao;
import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;

import static org.apache.cassandra.repair.scheduler.RepairUtil.getKsTbName;

public class LocalCassDaoImpl implements ILocalCassDao
{
    private final RepairSchedulerContext context;
    private final IRepairConfigDao repairConfigDao;

    public LocalCassDaoImpl(RepairSchedulerContext context, CassDaoUtil daoUtil)
    {
        this.context = context;
        this.repairConfigDao = new RepairConfigDaoImpl(context, daoUtil);
    }

    @Override
    public Set<Host> getAllHosts()
    {
        return context.localSession().getCluster().getMetadata().getAllHosts();
    }

    @Override
    public List<KeyspaceMetadata> getKeyspaces()
    {
        return context.localSession().getCluster().getMetadata().getKeyspaces();
    }

    /**
     * Checks whether a given keyspace is system related keyspace or not, scope of this function is to be used
     * explicitly for repair. This method considers `system_auth` and `system_distributed` as repairable
     * keyspaces in the explicit context of repair as these 2 keyspaces demand consistency with Network Topology
     * replications in production setups.
     *
     * @param name Name of the keyspace
     * @return boolean which indicates the keyspace is system/ repair-able
     */
    private boolean isRepairableKeyspace(String name)
    {
        return (
        name.equalsIgnoreCase("system") ||
        name.equalsIgnoreCase("system_traces") ||
        name.equalsIgnoreCase("dse_system") ||
        name.equalsIgnoreCase("system_schema")
        );
    }

    /**
     * Gets all repair enabled tables keyed by keyspace.table
     */
    @Override
    public List<TableRepairConfig> getAllRepairEnabledTables(String scheduleName)
    {
        // Get all tables by connecting local C* node and overlay that information with config information from
        // repair_config, using defaults for any tables not found in repair_config.
        Map<String, TableRepairConfig> returnMap = new HashMap<>();

        getKeyspaces().forEach(keyspaceMetadata -> keyspaceMetadata.getTables().forEach(tableMetadata -> {
            if (!isRepairableKeyspace(tableMetadata.getKeyspace().getName()))
            {
                String ksTbName = tableMetadata.getKeyspace().getName() + "." + tableMetadata.getName();
                TableRepairConfig tableConfig = new TableRepairConfig(context.getConfig(), scheduleName);

                // Repairing or compacting a TWCS or a DTCS is a bad idea, let's not do that.
                if (!tableMetadata.getOptions().getCompaction()
                                  .get("class").matches(".*TimeWindow.*|.*DateTiered.*"))
                {
                    tableConfig.setKeyspace(tableMetadata.getKeyspace().getName())
                               .setName(tableMetadata.getName())
                               .setTableMetadata(tableMetadata);

                    returnMap.put(ksTbName, tableConfig);
                }
            }
        }));

        // Apply any table specific overrides from the repair config
        List<TableRepairConfig> allConfigs = repairConfigDao.getRepairConfigs(context.getCassInteraction().getClusterName(), scheduleName);
        for (TableRepairConfig tcDb : allConfigs)
        {
            TableRepairConfig tableConfig = returnMap.get(getKsTbName(tcDb.getKeyspace(), tcDb.getName()));

            if (null != tableConfig)
            {
                tableConfig.clone(tcDb);
            }
        }
        return returnMap.values().stream().filter(TableRepairConfig::isRepairEnabled).collect(Collectors.toList());
    }


    @Override
    public Set<String> getAllRepairSchedules()
    {
        Set<String> repairSchedules = new HashSet<>();
        repairSchedules.add(context.getConfig().getDefaultSchedule());
        repairSchedules.addAll(repairConfigDao.getRepairSchedules(context.getCassInteraction().getClusterName()));
        return repairSchedules;
    }
}
