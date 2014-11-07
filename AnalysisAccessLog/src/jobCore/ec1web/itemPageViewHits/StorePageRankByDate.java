package jobCore.ec1web.itemPageViewHits;

import jobCore.ec1web.util.JsonKey;
import jobCore.ec1web.util.StoreApi;
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

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static jobCore.ec1web.util.Tool.*;
import static jobUtil.GlobalTool.*;

/**
 * 統計館頁的排名
 * Created by Joan on 2014/9/18.
 */
public class StorePageRankByDate {
    static final String className = StorePageRankByDate.class.getSimpleName();

    public static class StorePageRankByDateMapper extends Mapper<LongWritable, Text, Text, Text> {
        final String urlString = "http://ecshsolr.global.mypchome.com.tw:8080/solr/product";    // 線上 solr (slave)
        SolrServer solr = new HttpSolrServer(urlString);
        Text output_key = new Text(), output_value = new Text();
        Pattern pattern = Pattern.compile("record\\.htm?");
        Pattern store_pattern = Pattern.compile("SR_NO=\\w{6}|\\.tw/\\w{6}\"");
        Matcher m;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            if (!isBot(log) && pattern.matcher(log).find()) {   //不是機器人 && 符合點擊的 pattern
                String request, site, page, action, product, sr_no;
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
                        page = (String) json.get(JsonKey.Page.name());
                        action = (String) json.get(JsonKey.Action.name());
                        prod = (JSONArray) json.get(JsonKey.Prod.name());
                        // 決定點擊來源
                        site = (String) json.get(JsonKey.Site.name());
                        if (site != null && page != null && action != null) {
                            if (page.equals("store") && action.equals("pageview")) {
                                m = store_pattern.matcher(log);
                                output_key.set(site);
                                if (m.find()) {
                                    sr_no = m.group();
                                    sr_no = sr_no.contains("SR_NO") ? sr_no.replace("SR_NO=", "") : sr_no.replace(".tw/", "").replace("\"", "");
                                    output_value.set(sr_no);
                                    context.write(output_key, output_value);
                                } else {
                                    Object[] array = prod.toArray();
                                    product = array.length > 0 ? (String) array[0] : "";
                                    sr_no = getStoreNumberByItem(product);
                                    output_value.set(sr_no);
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

        private String getStoreNumberByItem(String it_sno) {
            String st_no = "";
            int index = it_sno.lastIndexOf("-");
            String it_no = it_sno.substring(0, index);
            String it_no_six = it_no.substring(0, 6);
            String q = "IT_NO:" + it_no;
            SolrQuery query = new SolrQuery();
            query.set("q", q);
            query.setRows(1);   //取一筆
            query.set("fl", "ST_NO");    //取 ST_NO 欄位
            SolrDocumentList results;
            try {
                QueryResponse response = solr.query(query); //送 query
                results = response.getResults();   //取結果
                if (results != null) {
                    if (results.size() > 0) {
                        st_no = (String) results.get(0).get("ST_NO");
                    }
                }
            } catch (SolrServerException e) {
                e.getStackTrace();
            }
            if (st_no.equals("") || st_no.length() != 6)
                st_no = it_no_six;
            return st_no;
        }
    }

    public static class StorePageRankByDateReducer extends Reducer<Text, Text, Text, NullWritable> {
        Text output_key = new Text();
        NullWritable nullWritable = NullWritable.get();
        HashMap<String, List<Map.Entry<String, Integer>>> result = new HashMap<String, List<Map.Entry<String, Integer>>>();
        HashMap<String, Integer> All_store_count = new HashMap<String, Integer>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String site, store_no;
            site = key.toString();
            HashMap<String, Integer> store_count = new HashMap<String, Integer>();
            Integer count, all_count;

            if (site.equals("shopping")) {
                if (result.containsKey("ecshop")) {
                    List<Map.Entry<String, Integer>> list = result.get("ecshop");
                    for (Map.Entry<String, Integer> entry : list)
                        store_count.put(entry.getKey(), entry.getValue());
                } else {
                    site = "ecshop";
                }
            }

            for (Text value : values) {
                store_no = value.toString();
                count = store_count.get(store_no);
                count = (count == null) ? 0 : count;
                store_count.put(store_no, count + 1);

                all_count = All_store_count.get(store_no);
                all_count = (all_count == null) ? 0 : all_count;
                All_store_count.put(store_no, all_count + 1);   //所有站台的館頁
            }

            result.put(site, sortMap(store_count)); // site -> (store, count)
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            StoreApi storeApi = new StoreApi();
            storeApi.openHttp();
            String[] site_arr = {"ecshop", "24h", "books", "globaltw", "globalen", "mobile", "ecssl", "ecvip"};
            List<Map.Entry<String, Integer>> All_list = sortMap(All_store_count);
            StringBuilder sb = new StringBuilder();
            //組欄位名稱
            sb.append("ALL-SR_CNAME").append(TAB).append("ALL-ST_NO").append(TAB).append("count");
            for (String site : site_arr) {
                sb.append(TAB).append(site).append("-SR_CNAME").append(TAB).append(site).append("-ST_NO").append(TAB).append("count");
            }

            output_key.set(sb.toString());
            context.write(output_key, nullWritable);
            sb.setLength(0);
            List<Map.Entry<String, Integer>> list;
            String store_no;
            // 組每一列的值
            for (int i = 0; i < All_list.size(); i++) {
                if (All_list.get(i).getValue() > 25) {
                    store_no = All_list.get(i).getKey();
                    storeApi.parseStoreNumber(store_no);
                    sb.append(storeApi.getSt_name()).append(TAB).append(store_no).append(TAB).append(All_list.get(i).getValue());
                    for (String site : site_arr) {
                        list = result.get(site);
                        if (list != null && list.size() > i) {
                            store_no = list.get(i).getKey();
                            storeApi.parseStoreNumber(store_no);
                            sb.append(TAB).append(storeApi.getSt_name()).append(TAB).append(store_no).append(TAB).append(list.get(i).getValue());
                        } else {
                            sb.append(TAB).append("").append(TAB).append("").append(TAB).append("");
                        }
                    }
                    output_key.set(sb.toString());
                    context.write(output_key, nullWritable);
                    sb.setLength(0);
                }
            }
            storeApi.closeHttp();   //連線關閉
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String input = args[0];
        String output = args[1];
        Path outputPath = new Path(output);

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

        Job job = new Job(conf, className + UNDERLINE + getJobDate(output));
        job.setJarByClass(StorePageRankByDate.class);
        job.setMapperClass(StorePageRankByDateMapper.class);
        job.setReducerClass(StorePageRankByDateReducer.class);
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