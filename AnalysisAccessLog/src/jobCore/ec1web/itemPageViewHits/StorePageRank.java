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
import static jobUtil.GlobalTool.TAB;
import static jobUtil.GlobalTool.UNDERLINE;

/**
 * 統計館頁的排名
 * Created by Joan on 2014/9/18.
 */
public class StorePageRank {
    static final String className = StorePageRank.class.getSimpleName();

    public static class StorePageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text output_key = new Text(), output_value = new Text();
        Pattern pattern = Pattern.compile("record\\.htm?");
        Pattern store_pattern = Pattern.compile("SR_NO=\\w{6}|\\.tw/\\w{6}\"");
        Matcher m;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            if (!isBot(log) && pattern.matcher(log).find()) {   //不是機器人 && 符合點擊的 pattern
                String request, site, page, action, sr_no;
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

                        // 決定點擊來源
                        site = (String) json.get(JsonKey.Site.name());
                        if (site != null) {
                            if (page.equals("store") && action.equals("pageview")) {
                                m = store_pattern.matcher(log);
                                if (m.find()) {
                                    sr_no = m.group();
                                    sr_no = sr_no.contains("SR_NO") ? sr_no.replace("SR_NO=", "") : sr_no.replace(".tw/", "").replace("\"", "");
                                    output_value.set(sr_no);
                                }
                                output_key.set(site);
                                context.write(output_key, output_value);
                            }
                        }
                    }
                }
            }
        }
    }

    public static class StorePageRankReducer extends Reducer<Text, Text, Text, NullWritable> {
        Text output_key = new Text();
        NullWritable nullWritable = NullWritable.get();
        HashMap<String, List<Map.Entry<String, Integer>>> result = new HashMap<String, List<Map.Entry<String, Integer>>>();
        HashMap<String, Integer> All_store_count = new HashMap<String, Integer>();
        public static final String rankTop = "rankTop";
        public static int TOP = 100;

        protected void setup(Context context) throws IOException, InterruptedException {
            TOP = context.getConfiguration().getInt(rankTop, TOP);
        }

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
            String store_no;    //品編
            int min = Math.min(TOP, All_list.size());
            // 組每一列的值
            for (int i = 0; i < min; i++) {
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
            storeApi.closeHttp();   //連線關閉
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String input = args[0];
        String output = args[1];
        Path outputPath = new Path(output);
        int reducerNum = (args.length >= 3) ? Integer.parseInt(args[2]) : 1;
        int top = (args.length >= 4) ? Integer.parseInt(args[3]) : 100;
        String time = args[4];

        Configuration conf = new Configuration();
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");   // 同一個job的task都在一個JVM執行:-1
        conf.setStrings("io.sort.mb", "150");
        conf.set("mapred.task.timeout", "18000000"); // ms (預設是 10 mins)
        conf.set("dfs.socket.timeout", "18000000");
        conf.set("dfs.datanode.socket.write.timeout", "18000000");
        conf.set("mapred.child.java.opts", "-Xmx512m -XX:+UseCompressedStrings -XX:-UseGCOverheadLimit -XX:+UseConcMarkSweepGC");
        conf.setInt("mapred.userlog.retain.hours", 72);
        conf.setInt(StorePageRankReducer.rankTop, top);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);  // output path 使用前先刪除

        Job job = new Job(conf, className + UNDERLINE + time);
        job.setJarByClass(StorePageRank.class);
        job.setMapperClass(StorePageRankMapper.class);
        job.setReducerClass(StorePageRankReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(reducerNum);

        FileInputFormat.addInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}