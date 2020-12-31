package canal.phoenix.client.adapter.test;

import canal.phoenix.client.adapter.config.ConfigurationManager;

/**
 * @author: lihua
 * @date: 2020/1/29 21:04
 * @Description:
 */
public class Test {

    public static void main(String[] args) {

        //System.out.println( ConfigurationManager.getInteger("regionCount"));
        System.out.println( ConfigurationManager.getInteger("threads"));
    }
}
