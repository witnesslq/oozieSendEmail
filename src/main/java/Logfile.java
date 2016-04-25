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

import java.io.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by sunteng on 2016/1/26.
 */
public class Logfile {
    private static Logger logger = LoggerFactory.getLogger(Logfile.class);


    static Configuration conf = new Configuration();
    static String file_profix = "http://master1:19888/ws/v1/history/mapreduce/jobs/";
    static List<String> filenames = new ArrayList<String>();
    static String jobid = "";
    static int success_file_count = 0;
    static int failed_file_count = 0;

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


    public static JsonObject handleResult(List<String> results) throws Exception {
        String job_id = null;
        String file_name = null;
        String task_id = null;
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
        String task = json.get("task").getAsJsonObject().get("state").getAsString();
        logger.info(task);
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

    public static void main(String[] paramert) throws Exception {

        File file = new File(System.getProperty("oozie.action.output.properties"));
        String baseDIR = paramert[0];
        String user = paramert[1];
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
        OutputStream os = new FileOutputStream(file);
        props.store(os, "");
        os.close();

        logger.info(props.toString());
    }
}



