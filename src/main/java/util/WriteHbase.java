package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sunteng on 2016/5/11.
 */
public class WriteHbase {
    private static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                "192.168.2.254,192.168.2.252,192.168.2.250");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    /**
     * 创建一个表
     *
     * @param tableName 表名字
     *                  <p/>
     */
    public static void createTable(String tableName) throws Exception {
        //admin 对象
        HBaseAdmin admin = new HBaseAdmin(conf);
        String[] columnFamilys = new String[]{"files_log"};

        if (admin.tableExists(tableName)) {
            System.out.println("此表，已存在！");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

            for (String columnFamily : columnFamilys) {
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            }

            admin.createTable(tableDesc);
            System.out.println("建表成功!");

        }
        admin.close();//关闭释放资源

    }

    /**
     * 批量添加数据
     *
     * @param tableName 标名字
     * @param rows      rowkey行健的集合
     *                  本方法仅作示例，其他的内容需要看自己义务改变
     *                  <p/>
     *                  *
     */
    public static void insertList(String tableName, String rows[]) throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Put> list = new ArrayList<Put>();
        for (String r : rows) {
            Put p = new Put(Bytes.toBytes(r));
            //此处示例添加其他信息
            p.add(Bytes.toBytes("family"), Bytes.toBytes("column"), 1000, Bytes.toBytes("value"));
            list.add(p);
        }
        table.put(list);//批量添加
        table.close();//释放资源
    }

    // 添加一条数据
    public static void addRow(String tableName, String row,
                              String columnFamily, String column, String value) throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(row));
        // 参数出分别：列族、列、值
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(value));
        table.put(put);
    }
}
