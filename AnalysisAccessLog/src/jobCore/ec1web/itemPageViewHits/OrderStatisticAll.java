package jobCore.ec1web.itemPageViewHits;

import jobCore.ec1web.util.JsonKey;
import jobCore.ec1web.util.StoreApi;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static jobCore.ec1web.util.Tool.*;
import static jobUtil.GlobalTool.*;

/**
 * 訂單統計 [10項]
 * 館編   館名  訂單量 ecshop_館頁_PV    24h_館頁_PV   行銷活動_PV   整點特賣_PV   books_PV    mobile_PV   全球中英文_館頁_PV
 * 預設數量是 0
 * Created by Joan on 2014/10/06.
 */
public class OrderStatisticAll {
    public final static String[] filed_arr = {"order", "24h_store", "ecshop_store", "activity", "onsale", "books", "mobile", "global_store"};

    /**
     * 蒐集訂單 && 活動 && 整點特賣 && 館頁
     * Page : activity, onsale ; Action : pageview
     * Page : checkout ; Action : order
     * <p/>
     * output → 品編  欄位名稱
     */
    public static class OrderStatisticAllMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text output_key = new Text();
        Text output_value = new Text();
        Pattern pattern = Pattern.compile("record\\.htm?");
        Pattern store_pattern = Pattern.compile("SR_NO=\\w{6}|\\.tw/\\w{6}\"");
        Matcher m;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            if (!isBot(log) && pattern.matcher(log).find()) {   //不是機器人 && 符合點擊的 pattern
                String request, site, page, action, sr_no = "", filed;
                JSONArray prod;
                request = getRequest(log);  //點擊的資料模型
                request = getJsonData(request); //只取 json 資料的部分
                if (!request.isEmpty()) {
                    request = URLDecoder.decode(request, ENCODING); //解碼
                    request = request.replaceAll("\"\"", "\""); //有些會有雙引號
                    JSONObject json = null;
                    try {
                        json = (JSONObject) JSONValue.parse(request);
                    } catch (NullPointerException e) {
                        System.out.println(request);
                        System.out.println(Arrays.toString(e.getStackTrace()));
                    }

                    if (json != null) {
                        HashSet<String> item_set = new HashSet<String>();
                        site = (String) json.get(JsonKey.Site.name());
                        page = (String) json.get(JsonKey.Page.name());
                        action = (String) json.get(JsonKey.Action.name());
                        prod = (JSONArray) json.get(JsonKey.Prod.name());
                        if (site != null && page != null && action != null) {
                            for (Object s : prod.toArray())
                                item_set.add(((String) s));

                            if (page.equals("store")) { //抓館編
                                m = store_pattern.matcher(log);
                                if (m.find()) {
                                    sr_no = m.group();
                                    sr_no = sr_no.contains("SR_NO") ? sr_no.replace("SR_NO=", "") : sr_no.replace(".tw/", "").replace("\"", "");
                                }
                            }

                            filed = getOutputKey(site, page, action);
                            if (!filed.isEmpty()) {
                                for (String item : item_set) {
                                    item = item.substring(0, item.lastIndexOf("-"));
                                    output_key.set(filed);
                                    output_value.set(item + TAB + sr_no);
                                    context.write(output_key, output_value);
                                }
                            }
                        } else {
                            System.out.println(request);
                        }
                    }
                }
            }
        }

        String getOutputKey(String site, String page, String action) {
            String s = "";
            // 24小時館頁PV
            if (site.equals("24h") && page.equals("store") && action.equals("pageview")) {
                s = site + UNDERLINE + page;
            }
            // 線上購物館頁PV
            else if (site.equals("ecshop") && page.equals("store") && action.equals("pageview")) {
                s = site + UNDERLINE + page;
            }
            // 書店PV
            else if (site.equals("books") && action.equals("pageview")) {
                s = site;
            }
            // 手機PV
            else if (site.equals("mobile") && action.equals("pageview")) {
                s = site;
            }
            // 行銷活動PV
            else if (page.equals("activity") && action.equals("pageview")) {
                s = page;
            }
            // 整點特賣PV
            else if (page.equals("onsale") && action.equals("pageview")) {
                s = page;
            }
            // 訂單
            else if (page.equals("checkout") && action.equals("order")) {
                s = action;
            }
            // global 館頁PV
            else if (page.equals("store") && action.equals("pageview")) {
                s = "global" + UNDERLINE + page;
            }
            return s;
        }
    }

    /**
     * input → site_page    IT_NO   ST_NO
     */
    public static class OrderStatisticAllReducer extends Reducer<Text, Text, Text, NullWritable> {
        Text output_key = new Text();
        NullWritable nullWritable = NullWritable.get();
        final String no_name = "NULL";
        public static HashMap<String, String> ST_NO_SR_CNAME_MAP = new HashMap<String, String>();

        final String urlString = "http://ecshsolr.global.mypchome.com.tw:8080/solr/product";    // 線上 solr (slave)
        final String test_urlString = "http://192.168.200.161:8080/solr-4.4.0/product"; // 外網測試用
        SolrServer solr;
        StoreApi storeApi;
        StringBuilder sb = new StringBuilder();
        HashMap<String, HashMap<String, Integer>> result = new HashMap<String, HashMap<String, Integer>>();
        HashMap<String, HashMap<String, Integer>> final_result = new HashMap<String, HashMap<String, Integer>>();

        protected void setup(Context context) throws IOException, InterruptedException {
            solr = new HttpSolrServer(urlString);
            storeApi = new StoreApi();
            storeApi.openHttp();
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> st_no_name_count = new HashMap<String, Integer>();
            String filed, data, it_no, st_no, it_no_six, st_name;
            Integer num;
            String[] it_st_no_array, result_array;
            filed = key.toString(); //也可以不轉
            for (Text value : values) {
                data = value.toString();    // data = IT_NO + TAB + ST_NO
                if (filed.equals("order")) {   //如果是 order，沒有館編，所以一定是要丟品編查館編和館名
                    it_st_no_array = data.split(TAB);
                    it_no = it_st_no_array[0];
                    it_no_six = it_no.substring(0, 6);  // 品編前六碼
                    result_array = getSolrQuery(it_no); // 取館編和館名
                    st_no = result_array[0].isEmpty() ? it_no_six : result_array[0];
                    st_name = result_array[1].isEmpty() ? null : result_array[1];
                    if (st_name == null) {
                        storeApi.parseStoreNumber(st_no);
                        st_no = storeApi.getSt_no().isEmpty() ? st_no : storeApi.getSt_no();
                        st_name = storeApi.getSt_name().isEmpty() ? no_name : storeApi.getSt_name();
                    }
                    ST_NO_SR_CNAME_MAP.put(st_no, st_name);   // 館編 → 館名
                    data = it_no + TAB + st_no;
                }

                num = st_no_name_count.get(data);
                if (num != null)
                    st_no_name_count.put(data, num + 1);
                else
                    st_no_name_count.put(data, 1);
            }

            result.put(filed, st_no_name_count);
        }

        /**
         * 加總次數 && 交接次數
         */
        private void counting() {
            HashMap<String, Integer> temp;
            HashMap<String, Integer> count_map;
            String it_no, st_no, st_name, count_map_key;
            String[] it_st_no_array;
            Integer number;
            for (String filed : filed_arr) {
                temp = result.get(filed);
                if (temp != null) {
                    count_map = new HashMap<String, Integer>(); //各 filed 接收加總的 map
                    for (String temp_key : temp.keySet()) {
                        it_st_no_array = StringUtils.split(temp_key, TAB);
                        it_no = it_st_no_array[0];
                        st_no = it_st_no_array.length == 2 ? it_st_no_array[1] : "";    //館頁就會有，但太多
                        if (st_no.isEmpty())
                            st_no = it_no.substring(0, 6);
                        st_name = ST_NO_SR_CNAME_MAP.get(st_no);
                        st_name = (st_name == null) ? no_name : st_name;
                        count_map_key = st_no + TAB + st_name;  //館編 + 館名 當 key
                        number = count_map.get(count_map_key);
                        if (number != null)
                            count_map.put(count_map_key, temp.get(temp_key) + number);
                        else
                            count_map.put(count_map_key, temp.get(temp_key));
                    }
                    final_result.put(filed, count_map);
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            counting();
            sb.append("ST_NO").append(TAB).append("SR_CNAME");
            for (String filed : filed_arr)
                sb.append(TAB).append(filed);
            output_key.set(sb.toString());
            context.write(output_key, nullWritable);
            sb.setLength(0);

            HashMap<String, Integer> temp;
            Integer num;
            List<Map.Entry<String, Integer>> order_list;
            try {
                order_list = sortMap(final_result.get(filed_arr[0]));  //對 order 做排序
                for (Map.Entry<String, Integer> entry : order_list) {   //只有取 order 的 key
                    sb.append(entry.getKey());
                    for (String filed : filed_arr) {
                        temp = final_result.get(filed);
                        if (temp != null) {
                            num = temp.get(entry.getKey());
                            num = num != null ? num : 0;
                        } else
                            num = 0;
                        sb.append(TAB).append(num);
                    }
                    output_key.set(sb.toString());
                    context.write(output_key, nullWritable);
                    sb.setLength(0);
                }
            } catch (NullPointerException e) {
                System.out.println(final_result.keySet());
                e.getStackTrace();
            }

            storeApi.closeHttp();   //連線關閉
        }

        private String[] getSolrQuery(String it_no) {
            String[] result_array = new String[2];
            String st_no = "", st_name = "";
            String q = "IT_NO:" + it_no;
            SolrQuery query = new SolrQuery();
            query.set("q", q);
            query.setStart(0);
            query.setRows(1);   //取一筆
            query.set("fl", "ST_NO SR_CNAME");    //取 ST_NO SR_CNAME 欄位
            SolrDocumentList results;
            try {
                QueryResponse response = solr.query(query); //送 query
                results = response.getResults();   //取結果
                if (results != null) {
                    if (results.size() > 0) {
                        st_no = (String) results.get(0).get("ST_NO");
                        st_name = (String) results.get(0).get("SR_CNAME");
                    }
                }
            } catch (SolrServerException e) {
                e.getStackTrace();
            }
            result_array[0] = st_no;
            result_array[1] = st_name;

            return result_array;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String input = args[0];
        String output = args[1];
        String job_output = output + File.separator + "OrderStatisticFinal";
        Path outputPath = new Path(job_output);
        String time = (args.length >= 3) ? args[2] : getJobDate(output);

        Configuration conf = new Configuration();
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");   // 同一個job的task都在一個JVM執行:-1
        conf.setStrings("io.sort.mb", "150");
        conf.set("mapred.task.timeout", "18000000"); // ms (預設是 10 mins)
        conf.set("dfs.socket.timeout", "18000000");
        conf.set("dfs.datanode.socket.write.timeout", "18000000");
        conf.set("mapred.child.java.opts", "-Xmx512m -XX:+UseCompressedStrings -XX:-UseGCOverheadLimit -XX:+UseConcMarkSweepGC");
        conf.setInt("mapred.userlog.retain.hours", 72);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);  // output path 使用前先刪除

        Job job = new Job(conf, OrderStatisticAll.class.getSimpleName() + UNDERLINE + time);
        job.setJarByClass(OrderStatisticAll.class);
        job.setMapperClass(OrderStatisticAllMapper.class);
        job.setReducerClass(OrderStatisticAllReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}