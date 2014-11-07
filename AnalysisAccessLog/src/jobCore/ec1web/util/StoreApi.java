package jobCore.ec1web.util;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.*;
import java.util.Date;

import static jobUtil.GlobalTool.TAB;

/**
 * 使用 API
 * 傳館編取得館名
 * http://ecapi.pchome.com.tw/ecshop/cateapi/v1.4/store&id=BMAB7Q&fields=Id,Name&_callback=12345
 * _callback 值可任意代
 * Created by Joan on 2014/10/7.
 */
public class StoreApi {
    HttpClient httpClient;
    final String encoding = "utf-8";
    final String url = "http://ecapi.pchome.com.tw/ecshop/cateapi/v1.4/store&id=";
    final String url_ned = "&fields=Id,Name&_callback=";
    String st_no;
    String st_name;

    public static void main(String[] args) throws UnsupportedEncodingException {
        String st_no = "BMAB7Q";
        StoreApi storeApi = new StoreApi();
        storeApi.openHttp();
        storeApi.parseStoreNumber(st_no);
        System.out.println(storeApi.getSt_no());
        System.out.println(storeApi.getSt_name());
        storeApi.closeHttp();
    }

    public String getSt_no() {
        return st_no;
    }

    public String getSt_name() {
        return StringEscapeUtils.unescapeHtml4(st_name).replaceAll(TAB, "");    //解 html 編碼
    }

    /**
     * 將回傳的字串做解碼
     * 取得館編和館名
     *
     * @param response 1412733343447([{"Id":"BMAB7Q","Name":"\u51ac\u5b63\u9ad8\u7d1a\u578b\u7cfb\u5217"}]);
     */
    private void responseDecode(String response) {
        int index_start = response.indexOf("[");
        int index_end = response.lastIndexOf(")");
        String s = response.substring(index_start, index_end);
        if (!s.isEmpty() && s.length() > 2) {
            JSONArray jsonArray = (JSONArray) JSONValue.parse(s);
            if (jsonArray != null) {
                JSONObject jsonObject = (JSONObject) jsonArray.get(0);
                st_no = (String) jsonObject.get("Id");
                st_name = (String) jsonObject.get("Name");
            } else {
                System.out.println("json parse fail : " + response);
                initial();
            }
        } else {
            System.out.println("json match fail : " + response);
            initial();
        }
    }

    public void parseStoreNumber(String store_number) {
        long callback = getRandomCallbackNo();
        StringBuilder sb = new StringBuilder();
        store_number = store_number.toUpperCase();
        String request = url + store_number + url_ned + callback;
        HttpGet httpGet = new HttpGet(request);
//        httpGet.setHeader("Accept", "Accept text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
        httpGet.setHeader("Accept", "Accept text/html");    //加 "Accept" 是必要的，否則會被導向首頁
        httpGet.setHeader("Accept-Charset", encoding);
        httpGet.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36");
        HttpResponse response = null;
        try {
            response = httpClient.execute(httpGet);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (response != null) {
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                try {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        BufferedReader reader;
                        InputStream in = entity.getContent();
                        reader = new BufferedReader(new InputStreamReader(in, encoding));
                        while (true) {
                            String l = reader.readLine();
                            if (l == null) {
                                break;
                            }
                            sb.append(l);
                        }
                        reader.close();
                        in.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        if (sb.toString().isEmpty()) {
            System.out.println("web page parse fail : " + store_number);
            initial();
        } else {
            responseDecode(sb.toString());
        }
    }

    /**
     * 關閉 http 連線
     */
    public void closeHttp() {
        httpClient.getConnectionManager().shutdown();
    }

    /**
     * 啟動 http 連線
     */
    public void openHttp() {
        httpClient = new DefaultHttpClient();
    }

    private long getRandomCallbackNo() {
        return new Date().getTime();
    }

    private void initial() {
        st_no = "";
        st_name = "";
    }
}
