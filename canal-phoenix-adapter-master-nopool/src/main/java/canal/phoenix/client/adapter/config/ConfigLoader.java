package canal.phoenix.client.adapter.config;

import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Phoenix表映射配置加载器
 * 和adapter rdb一模一样
 */
public class ConfigLoader {

    private static Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    /**
     * 加载Phoenix表映射配置
     *
     * @return 配置名/配置文件名--对象
     */
    public static Map<String, MappingConfig> load(Properties envProperties) {
        logger.info("## Start loading phoenix mapping config ... ");

        Map<String, MappingConfig> result = new LinkedHashMap<>();

        Map<String, String> configContentMap = MappingConfigsLoader.loadConfigs("phoenix");
        configContentMap.forEach((fileName, content) -> {
            MappingConfig config = YmlConfigBinder
                    .bindYmlToObj(null, content, MappingConfig.class, null, envProperties);
            if (config == null) {
                return;
            }
            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + fileName + " " + e.getMessage(), e);
            }
            result.put(fileName, config);
        });

        logger.info("## Phoenix mapping config loaded");
        //手动增加的
        logger.info("## Phoenix同步线程数: " + ConfigurationManager.getInteger("threads"));
        return result;
    }
}
