package util;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by sunteng on 2016/5/11.
 */
public class ProUtil {
    public Properties getProperties() {
        Properties p = new Properties();
        try {
            p.load(ProUtil.class.getClassLoader().getResourceAsStream("hbase.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return p;
    }

}
