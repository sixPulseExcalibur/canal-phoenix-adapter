package canal.phoenix.client.adapter;

import com.alibaba.druid.pool.ExceptionSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Properties;

/**
 * @author: lihua
 * @date: 2020/3/30 13:10
 * @Description:
 */
public class PhoenixExceptionSorter implements ExceptionSorter {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixExceptionSorter.class);
    @Override
    public boolean isExceptionFatal(SQLException e) {
        if (e.getMessage().contains("Connection is null or closed")) {
         logger.error("剔除phoenix不可用的连接", e);
        return true;
       }
        return false;
    }

    @Override
    public void configFromProperties(Properties properties) {

    }
}
