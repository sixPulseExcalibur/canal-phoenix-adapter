package canal.phoenix.client.adapter;

import canal.phoenix.client.adapter.config.ConfigLoader;
import canal.phoenix.client.adapter.config.MappingConfig;
import canal.phoenix.client.adapter.monitor.PhoenixConfigMonitor;
import canal.phoenix.client.adapter.service.PhoenixEtlService;
import canal.phoenix.client.adapter.service.PhoenixSyncService;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import canal.phoenix.client.adapter.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Phoenix适配器实现类
 */
@SPI("phoenix")
public class PhoenixAdapter implements OuterAdapter {

    private static Logger logger = LoggerFactory.getLogger(PhoenixAdapter.class);

    private Map<String, MappingConfig> phoenixMapping = new ConcurrentHashMap<>();                // 文件名对应配置
    private Map<String, Map<String, MappingConfig>> mappingConfigCache = new ConcurrentHashMap<>();                // 库名-表名对应配置

    private DruidDataSource dataSource;

    private PhoenixSyncService phoenixSyncService;

    private PhoenixConfigMonitor phoenixConfigMonitor;

    private Properties envProperties;

    public Map<String, MappingConfig> getPhoenixMapping() {
        return phoenixMapping;
    }

    public Map<String, Map<String, MappingConfig>> getMappingConfigCache() {
        return mappingConfigCache;
    }

    public PhoenixAdapter() {
        logger.info("PhoenixAdapter create: {} {}", this, Thread.currentThread().getStackTrace());
    }
    /**
     * 初始化方法
     *
     * @param configuration 外部适配器配置信息
     */
    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        this.envProperties = envProperties;
        Map<String, MappingConfig> phoenixMappingTmp = ConfigLoader.load(envProperties);
        // 过滤不匹配的key的配置
        phoenixMappingTmp.forEach((key, mappingConfig) -> {
            if ((mappingConfig.getOuterAdapterKey() == null && configuration.getKey() == null)
                    || (mappingConfig.getOuterAdapterKey() != null
                    && mappingConfig.getOuterAdapterKey().equalsIgnoreCase(configuration.getKey()))) {
                phoenixMapping.put(key, mappingConfig);
            }
        });

        if (phoenixMapping.isEmpty()) {
            throw new RuntimeException("No phoenix adapter found for config key: " + configuration.getKey());
        } else {
            logger.info("[{}]phoenix config mapping: {}", this, phoenixMapping.keySet());
        }

        for (Map.Entry<String, MappingConfig> entry : phoenixMapping.entrySet()) {
            String configName = entry.getKey();
            MappingConfig mappingConfig = entry.getValue();
            String key;
            if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "-"
                        + StringUtils.trimToEmpty(mappingConfig.getGroupId()) + "_"
                        + mappingConfig.getDbMapping().getDatabase() + "-" + mappingConfig.getDbMapping().getTable().toLowerCase();
            } else {
                key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "_"
                        + mappingConfig.getDbMapping().getDatabase() + "-" + mappingConfig.getDbMapping().getTable().toLowerCase();
            }
            Map<String, MappingConfig> configMap = mappingConfigCache.computeIfAbsent(key,
                    k1 -> new ConcurrentHashMap<>());
            configMap.put(configName, mappingConfig);
        }

        // 初始化连接池
        Map<String, String> properties = configuration.getProperties();
        try {
        dataSource = new DruidDataSource();
        //读取配置文件
        dataSource.setDriverClassName(properties.get("jdbc.driverClassName"));
        dataSource.setUrl(properties.get("jdbc.url"));
        dataSource.setUsername(properties.get("jdbc.username"));
        dataSource.setPassword(properties.get("jdbc.password"));
        // 池启动时创建的连接数量
        dataSource.setInitialSize(1);
        //在不新建连接的条件下，池中保持空闲的最少连接数。
        dataSource.setMinIdle(1);
        //同一时间可以从池分配的最多连接数量。设置为0时表示无限制。
        dataSource.setMaxActive(30);
        //在抛出异常之前，池等待连接被回收的最长时间（当没有可用连接时）。设置为-1表示无限等待。
        dataSource.setMaxWait(60000);
        // destory线程检测时间,隔多久检测一次连接有效性(单位:毫秒)
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        // 连接生存最小时间(单位 :毫秒)
        dataSource.setMinEvictableIdleTimeMillis(300000);
        //连接等待超时时间 单位为毫秒 缺省启用公平锁，
        //并发效率会有所下降， 如果需要可以通过配置useUnfairLock属性为true使用非公平锁
        dataSource.setUseUnfairLock(true);

        // 建议配置为true，不影响性能，并且保证安全性。申请连接的时候检测，如果空闲时间大于timeBetweenEvictionRunsMillis，执行validationQuery检测连接是否有效。
            //testWhileIdle is true, validationQuery not set
            dataSource.setTestWhileIdle(true);
            //验证连接是否可用，使用的SQL语句
            dataSource.setValidationQuery("SELECT 1");
        //对于长时间不使用的连接强制关闭,默认false
        //dataSource.setRemoveAbandoned(true);
        //关闭超过24小时的空闲连接，24*3600s 此时间应大于业务运行的最长时间
        //超过时间限制，回收没有用(废弃)的连接（默认为 300秒，调整为24*3600s）
        //dataSource.setRemoveAbandonedTimeout(24 * 3600);
        //设置剔除异常连接机制
        dataSource.setExceptionSorter(new PhoenixExceptionSorter());

/*
        dataSource.setQueryTimeout(60);
        //是否缓存preparedStatement，也就是PSCache。PSCache对支持游标的数据库性能提升巨大，比如说oracle。在mysql下建议关闭。 默认为false
        dataSource.setPoolPreparedStatements(true);
        //要启用PSCache，必须配置大于0，当大于0时，poolPreparedStatements自动触发修改为true。在Druid中，不会存在Oracle下PSCache占用内存过多的问题，可以把这个数值配置大一些，比如说100
        dataSource.setMaxOpenPreparedStatements(50);
        //申请连接时执行validationQuery检测连接是否有效，配置为true会降低性能;  默认为true
        //只会发现当前连接失效，再创建一个连接供当前查询使用
        dataSource.setTestOnBorrow(false);
        //归还连接时执行validationQuery检测连接是否有效，配置为true会降低性能;  默认为false
        dataSource.setTestOnReturn(false);
        //打开abanded连接时输出错误日志
        dataSource.setLogAbandoned(false);
        // 启用监控统计功能
        dataSource.setFilters("stat");
         //程序中的连接不使用后是否被连接池回收
         dataSource.setRemoveAbandonedOnBorrow(true);
         dataSource.setRemoveAbandonedOnMaintenance(true);
*/

        // array.add("set names utf8mb4;");
        // dataSource.setConnectionInitSqls(array);

            dataSource.init();
        } catch (SQLException e) {
            logger.error("ERROR ## failed to initial datasource: " + properties.get("jdbc.url"), e);
        }

        String threads = properties.get("threads");
        // String commitSize = properties.get("commitSize");

        phoenixSyncService = new PhoenixSyncService(dataSource,
                threads != null ? Integer.valueOf(threads) : null
        );

        phoenixConfigMonitor = new PhoenixConfigMonitor();
        phoenixConfigMonitor.init(configuration.getKey(), this, envProperties);
    }





    /**
     * 同步方法
     *
     * @param dmls 数据包
     */
    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        try {
            phoenixSyncService.sync(mappingConfigCache, dmls, envProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ETL方法
     *
     * @param task   任务名, 对应配置名
     * @param params etl筛选条件
     * @return ETL结果
     */
    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        MappingConfig config = phoenixMapping.get(task);
        if (config != null) {
            DataSource srcDataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            if (srcDataSource != null) {
                return PhoenixEtlService.importData(srcDataSource, dataSource, config, params);
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSucc = true;
            // ds不为空说明传入的是destination
            for (MappingConfig configTmp : phoenixMapping.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    DataSource srcDataSource = DatasourceConfig.DATA_SOURCES.get(configTmp.getDataSourceKey());
                    if (srcDataSource == null) {
                        continue;
                    }
                    EtlResult etlRes = PhoenixEtlService.importData(srcDataSource, dataSource, configTmp, params);
                    if (!etlRes.getSucceeded()) {
                        resSucc = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSucc);
                if (resSucc) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }

    /**
     * 获取总数方法
     *
     * @param task 任务名, 对应配置名
     * @return 总数
     */
    @Override
    public Map<String, Object> count(String task) {
        Map<String, Object> res = new LinkedHashMap<>();
        MappingConfig config = phoenixMapping.get(task);
        if (config == null) {
            logger.info("[{}]phoenix config mapping: {}", this, phoenixMapping.keySet());
            res.put("succeeded", false);
            res.put("errorMessage", "Task[" + task + "] not found");
            res.put("tasks", phoenixMapping.keySet());
            return res;
        }
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        String sql = "SELECT COUNT(1) AS cnt FROM " + SyncUtil.getDbTableName(dbMapping);
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            Util.sqlRS(conn, sql, rs -> {
                try {
                    if (rs.next()) {
                        Long rowCount = rs.getLong("cnt");
                        res.put("count", rowCount);
                    }
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            });
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        res.put("targetTable", SyncUtil.getDbTableName(dbMapping));

        return res;
    }

    /**
     * 获取对应canal instance name 或 mq topic
     *
     * @param task 任务名, 对应配置名
     * @return destination
     */
    @Override
    public String getDestination(String task) {
        MappingConfig config = phoenixMapping.get(task);
        if (config != null) {
            return config.getDestination();
        }
        return null;
    }

    /**
     * 销毁方法
     */
    @Override
    public void destroy() {
        if (phoenixConfigMonitor != null) {
            phoenixConfigMonitor.destroy();
        }

        if (phoenixSyncService != null) {
            phoenixSyncService.close();
        }

        if (dataSource != null) {
            dataSource.close();
        }
    }
}
