package util;


import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * Created by sunteng on 2016/6/8.
 */
public class HttpUtil {
    private static final String USER_AGENT = "Mozilla/5.0";

    /**
     * @param url
     * @param parms
     * @throws Exception
     */
    public static void sendPost(String url, String parms) throws Exception {
//        HttpGet httpget = new HttpGet(baseurl);

//        CloseableHttpClient httpclient = HttpClients.createDefault();
        //2.生成一个get请求
        HttpGet httpget = new HttpGet(url + "/" + parms);
        //3.执行get请求并返回结果
//        CloseableHttpResponse response = httpclient.execute(httpget);
        HttpResponse response = new DefaultHttpClient().execute(httpget);
        try {
            //4.处理结果
        } finally {

        }
    }

}
