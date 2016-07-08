import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.*;

import java.io.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 邮件发送以及
 * 数据插入hbase
 * Created by sunteng on 2016/1/26.
 */
public class Logfile {
    private static Logger logger = LoggerFactory.getLogger(Logfile.class);

    //属性指定
    private static Configuration conf = new Configuration();
    private static String file_profix = Constant.FILE_PROFIX;
    private static List<String> filenames = new ArrayList<String>();
    private static String jobid = "";
    private static int success_file_count = 0;
    private static int failed_file_count = 0;
    private static String task_id = null;

    //工作流执行结果
    private static String task = "";
    private static String start_time = "";
    private static String end_time = "";


    /**
     * 查找指定目下hdfs文件
     *
     * @param baseDirName
     * @param fileList
     * @throws Exception
     */
    public static void findFiles(String baseDirName, List fileList) throws Exception {
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

    /**
     * 处理获取结果数据
     *
     * @param results
     * @return
     * @throws Exception
     */
    public static JsonObject handleResult(List<String> results) throws Exception {
        String job_id = null;
        String file_name = null;
        for (int i = 0; i < results.size(); i++) {
            if (results.get(i).startsWith("job_")) {
                job_id = results.get(i);
                jobid = job_id;
            } else if (results.get(i).startsWith("task_")) {
                task_id = results.get(i);
            } else {
                file_name = results.get(i);

            }
        }
        String path = file_profix + job_id + "/" + "tasks/" + task_id;
        JsonObject json = readJsonFromUrl(path);
        JsonObject resultJson = json.get("task").getAsJsonObject();
        task = resultJson.get("state").getAsString();
        start_time = resultJson.get("startTime").getAsString();
        end_time = resultJson.get("finishTime").getAsString();

        logger.info(task + start_time + end_time);
        boolean flag = task.equals("SUCCEEDED");
        logger.info(flag + "");
        if (flag == false) {
            filenames.add(file_name);
            failed_file_count++;
        } else if (flag == true) {
            success_file_count++;
        }
        logger.info(filenames.toString());
        return json;
    }

    /**
     * 读文件
     *
     * @param location
     * @return
     * @throws Exception
     */
    public static List<String> readLines(Path location) throws Exception {
        List<String> results = new ArrayList<String>();
        FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);

        CompressionCodec codec = factory.getCodec(location);
        InputStream stream = null;

        // check if we have a compression codec we need to use
        if (codec != null) {
            stream = codec.createInputStream(fileSystem.open(location));
        } else {
            stream = fileSystem.open(location);
        }

        StringWriter writer = new StringWriter();
        IOUtils.copy(stream, writer, "UTF-8");
        String raw = writer.toString();
        for (String str : raw.split("\n")) {
            results.add(str);
        }
        logger.info(results.toString());

        return results;
    }

    /**
     * 读数据
     *
     * @param baseurl
     * @return
     * @throws IOException
     */
    public static JsonObject readJsonFromUrl(String baseurl) throws IOException {
        JsonObject json = new JsonObject();
        Gson gson = new Gson();

        HttpGet httpget = new HttpGet(baseurl);

        HttpResponse response = new DefaultHttpClient().execute(httpget);

        HttpEntity entity = response.getEntity();
        String source = EntityUtils.toString(entity);

        if (entity != null) {
            json = gson.fromJson(source, JsonObject.class);
            logger.info(json.toString());
        }
        return json;
    }

    /**
     * 程序入口
     *
     * @param paramert
     * @throws Exception
     */
    public static void main(String[] paramert) throws Exception {

        File file = new File(System.getProperty("oozie.action.output.properties"));
        String baseDIR = paramert[0];
        String user = paramert[1];
        String output_dir = paramert[2];
        baseDIR = baseDIR + "/" + user;
        List<FileStatus> resultList = new ArrayList<FileStatus>();
        findFiles(baseDIR, resultList);

        if (resultList.size() == 0) {
            logger.info("No File Found.");
        } else {
            for (int i = 0; i < resultList.size(); i++) {
                List<String> results = readLines(resultList.get(i).getPath());
                JsonObject json = handleResult(results);
                logger.info(json.toString());
            }
        }
        for (int i = 0; i < filenames.size(); i++) {
            logger.info(filenames.get(i).toString());
        }
        Properties props = new Properties();
        props.setProperty("filenames", filenames.toString());
        props.setProperty("job_id", jobid);
        String log_url = "http://master1:19888/jobhistory/job/" + jobid + "/" + "jobhistory/job" + "/" + jobid;
        props.setProperty("failed_file_count", failed_file_count + "");
        props.setProperty("success_file_count", success_file_count + "");
        props.setProperty("log_url", log_url);
        String log_summary_url = Constant.LOG_SUNMMARY_URL;
        String task_log_url = Constant.TASK_LOG_URL;


        List<FileStatus> outputList = new ArrayList<FileStatus>();
        findFiles(output_dir, outputList);

        StringBuffer sb = new StringBuffer();
        if (outputList.size() > 0) {
            for (int i = 0; i < outputList.size(); i++) {
                sb.append(outputList.get(i).getPath().getName().toString()).append(",");

            }
        }

        String split = Constant.SPLIT_CH;
        String split_jt = Constant.SPLIT_JOBTASK;

        //服务入hbase
        if (failed_file_count > 0) {
            new HttpUtil().sendPost(log_summary_url, jobid + split_jt + task_id + split + start_time + split + end_time + split + "FAILED" + split + sb.toString());
            new HttpUtil().sendPost(task_log_url, user + split + jobid + split + start_time + split + end_time + split + "FAILED");

        } else {
            new HttpUtil().sendPost(log_summary_url, jobid + split_jt + task_id + split + start_time + split + end_time + split + "SUCCEED" + split + sb.toString());
            new HttpUtil().sendPost(task_log_url, user + split + jobid + split + start_time + split + end_time + split + "SUCCEED");
        }

        OutputStream os = new FileOutputStream(file);
        props.store(os, "");
        os.close();

    }
}



