package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sunteng on 2016/6/7.
 */
public class OoizeOutFilesGet {
    static Configuration conf = new Configuration();
    private static Logger logger = LoggerFactory.getLogger(OoizeOutFilesGet.class);

    /**
     * 获取目录hdfs文件
     * @param baseDirName
     * @param fileList
     * @throws Exception
     */
    public static void findFiles(String baseDirName, List<FileStatus> fileList) throws Exception {
        boolean flag;
        baseDirName = baseDirName;
        Path srcPath = new Path(baseDirName);
        FileSystem srcFs = srcPath.getFileSystem(conf);

        flag = srcFs.exists(srcPath);
        if (!flag) {
            logger.info(baseDirName);
        } else {
            FileStatus status[] = srcFs.listStatus(srcPath);
            for (int i = 0; i < status.length; i++) {
                if (!status[i].isDirectory()) {
                    fileList.add(status[i]);
                } else if (status[i].isDirectory()) {
                    findFiles(status[i].getPath().toString(), fileList);
                }
            }
        }
    }
}
