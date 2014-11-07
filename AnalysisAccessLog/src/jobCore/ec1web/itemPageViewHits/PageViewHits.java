package jobCore.ec1web.itemPageViewHits;

import gnu.trove.map.hash.TObjectIntHashMap;
import jobCore.ec1web.util.Device;
import jobCore.ec1web.util.JsonKey;
import jobCore.ec1web.util.Record;
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
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Pattern;

import static jobCore.ec1web.util.Tool.*;
import static jobUtil.GlobalTool.*;

/**
 * 分析 ec1web access log
 * 統計商品點擊紀錄
 * log 範例 :
 * Sep  4 00:00:05 ec1web11 apache2: 122.117.188.109 - - [04/Sep/2014:00:00:05 +0800] "GET /emon/v2/record.htm?%7B%22Uid%22%3A%220c0ad73c7f80da060912e0fbd474092428456560%22%2C%22Site%22%3A%22ecshop%22%2C%22Page%22%3A%22store%22%2C%22Action%22%3A%22pageview%22%2C%22Prod%22%3A%5B%22AEAB5L-A74589207-000%22%5D%7D&_=1409760045292&callback=jQuery17104508145914878696_1409760042103 HTTP/1.1" 200 26 "http://shopping.pchome.com.tw/AEAB5L" "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.172 Safari/537.22" uid=689c2e8658bfc5576f20cd926e9ee55c ecc=4d9dec0088871ff77bf1c7663235a32a
 * Created by Joan on 2014/9/5.
 */
public class PageViewHits {
    public static class PageViewHitsMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text output_key = new Text(), output_value = new Text();
        Pattern pattern = Pattern.compile("record\\.htm?");
        final String SPACE = "[\\s\\-_]?";

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            if (!isBot(log) && pattern.matcher(log).find()) {   //不是機器人 && 符合點擊的 pattern
                StringBuilder sb = new StringBuilder();
                String request, ip, site, page, action;
                Device device;
                ip = getIP(log);
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
                            log = log.toLowerCase();    //統一小寫
                            // 特定IP→ amazon, portal
                            if (ip.equals("54.225.101.193") || ip.equals("54.235.93.182") || ip.contains("122.147.50.")) {
                                device = Device.Other;
                                // 先處理 apple 產品, 因為較規律. ipad, 小米 pad, 華為 pad, coolpad, lenovo pad, 黑莓 pad, lg pad
                            } else if (isKnownDevice(log, Pattern.compile("\"[^\\(]+\\(ipad;|tablet pc|mi pad|huaweimediapad|coolpad\\s?9976|ideatab|lenovo" + SPACE + "[a-z]\\d{4}|playbook|lg\\-v"))) {
                                device = Device.Tablet;
                            } else if (isKnownDevice(log, Pattern.compile("\"[^\\(]+\\(iphone;|\"[^\\(]+\\(ipod;|padfone|windows phone|j2me/midp|coolpad|lenovo" + SPACE + "[a-z]\\d{3}|bb10|blackberry|lg\\-|nexus[\\s\\-_]?5"))) {
                                device = Device.Phone;
                            } else if (isKnownDevice(log, Pattern.compile("\"[^\\(]+\\(macintosh;|windows|ubuntu|aspire v5-132p|\\s?cros\\s"))) {
                                device = Device.PC;
                            }
                            // 小米手機 紅米手機
                            else if (isKnownDevice(log, Pattern.compile("mi" + SPACE + "\\d|hm" + SPACE + "note|hm\\s?\\d|mi" + SPACE + "one"))) {
                                device = Device.Phone;
                            }
                            // 平板 samsung: sm\-[tp]|gt\-n[58]|gt\-p , sony: sgpt12|sgp\d{3} , asus: k0[01]\w|tf\d{3}|me[13][07][1-3]\w , htc: (htc)?[\s-_]?flyer
                            else if (isKnownDevice(log, Pattern.compile("sm\\-[tp]|gt\\-n[58]|gt\\-p|sgpt12|sgp\\d{3}|k0[01]\\w|tf\\d{3}|me[13][07][1-3]\\w|transformer|nexus[\\s\\-_]?[7-9]|nexus[\\s\\-_]?10|(htc)?[\\s\\-_]?flyer"))) {
                                device = Device.Tablet;
                            }
                            // 手機 samsung: gt\-i|sch\-i9|sgh\-n|sm\-[ng]|gt\-n7 ,sony: lt\d{2}[a-z]|c2[13]05|c[35689]{2}0[23]|d[56][35][06][36] , asus: t00\w , htc: htc
                            else if (isKnownDevice(log, Pattern.compile("gt\\-i|sch\\-i9|sgh\\-n|sm\\-[ng]|gt\\-n7|lt\\d{2}[a-z]|c2[13]05|c[35689]{2}0[23]|d[56][35][06][36]|t00\\w|htc|infocus|vovo"))) {
                                device = Device.Phone;
                            }
                            // 其他牌已知平板
                            else if (isKnownDevice(log, Pattern.compile("t518|hp\\-tablet|mypad|gsmart7tab|tegranote\\-p1640|hd\\-89"))) {
                                device = Device.Tablet;
                            } // 其他牌未知平板
                            else if (isKnownDevice(log, Pattern.compile("tablet|pad"))) {
                                device = Device.Tablet;
                            } // 剩餘手機
                            else if (isKnownDevice(log, Pattern.compile("mobile|android"))) {
                                device = Device.Phone;
                            } // 機器人
                            else if (isKnownDevice(log, Pattern.compile("bot|spider"))) {
                                device = Device.Other;
                            }// PC
                            else if (isKnownDevice(log, Pattern.compile("linux"))) {
                                device = Device.PC;
                            } else {
                                device = Device.Unknown;
                            }
                            sb.append(device.name()).append(TAB).append(page).append(TAB).append(action);

                            if (!site.isEmpty() && sb.length() > 0) {
                                output_key.set(site);
                                output_value.set(sb.toString());
                                context.write(output_key, output_value);
                            }
                        } else {
                            System.out.println("site is null : " + log);
                        }
                        sb.setLength(0);
                    }
                }
            }
        }
    }

    /**
     * input : key -> site  value -> device + page + action
     */
    public static class PageViewHitsReducer extends Reducer<Text, Text, Text, NullWritable> {
        Text output_key = new Text();
        NullWritable nullWritable = NullWritable.get();
        ArrayList<String> result = new ArrayList<String>();

        protected void setup(Context context) throws IOException, InterruptedException {
            result.add("site\tpage\taction\tnormalPageTotal\tmobilePageTotal\tPC\tPhone\ttablet\tother\tunknown");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String site, device, page, active, data, mapping_key;
            site = key.toString();
            String[] arr;
            TObjectIntHashMap<String> record_count = new TObjectIntHashMap<String>();
            HashMap<String, Record> mapping = new HashMap<String, Record>();

            for (Text value : values) {
                data = value.toString();
                record_count.adjustOrPutValue(data, 1, 1);
            }

            Record record;
            Device switch_key;
            for (String s : record_count.keySet()) {
                int count = record_count.get(s);
                arr = StringUtils.split(s, TAB);
                device = arr[0];
                page = arr[1];
                active = arr[2];
                mapping_key = site + page + active;
                record = mapping.get(mapping_key);
                if (record == null)
                    record = new Record(site, page, active);
                switch_key = Device.valueOf(device);
                switch (switch_key) {
                    case PC:
                        record.plusPCcount(count);
                        break;
                    case Phone:
                        record.plusPhoneCount(count);
                        break;
                    case Tablet:
                        record.plusTabletCount(count);
                        break;
                    case Other:
                        record.plusOtherCount(count);
                        break;
                    case Unknown:
                        record.plusUnknownCount(count);
                        break;
                }
                mapping.put(mapping_key, record);
            }

            for (Record r : mapping.values()) {
                result.add(r.toString());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String s : result) {
                output_key.set(s);
                context.write(output_key, nullWritable);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String input = args[0];
        String output = args[1];
        String job1_output = output + File.separator + "ALL";
        String job2_output = output + File.separator + "CatePVStatistic";
        String job4_output = output + File.separator + "OrderStatisticFinal";
        Path job1_outputPath = new Path(job1_output);
        Path job2_outputPath = new Path(job2_output);
        Path job4_outputPath = new Path(job4_output);
        String date = getJobDate(output);

        Configuration conf = new Configuration();
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");   // 同一個job的task都在一個JVM執行:-1
        conf.setStrings("io.sort.mb", "150");
        conf.set("mapred.task.timeout", "18000000"); // ms (預設是 10 mins)
        conf.set("dfs.socket.timeout", "18000000");
        conf.set("dfs.datanode.socket.write.timeout", "18000000");
        conf.set("mapred.child.java.opts", "-Xmx512m -XX:+UseCompressedStrings -XX:-UseGCOverheadLimit -XX:+UseConcMarkSweepGC");
        conf.setInt("mapred.userlog.retain.hours", 72);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(job1_outputPath, true);  // output path 使用前先刪除
        fs.delete(job2_outputPath, true);  // output path 使用前先刪除
        fs.delete(job4_outputPath, true);  // output path 使用前先刪除

        //-----[job1] 全部資料
        Job job = new Job(conf, PageViewHits.class.getSimpleName() + UNDERLINE + date);
        job.setJarByClass(PageViewHits.class);
        job.setMapperClass(PageViewHitsMapper.class);
        job.setReducerClass(PageViewHitsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        ControlledJob cJob1 = new ControlledJob(job.getConfiguration());
        cJob1.setJob(job);

        FileInputFormat.addInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, job1_outputPath);

        //-----[job2] 整理 page view 數據
        Job job2 = new Job(conf, CatePVStatistic.class.getSimpleName() + UNDERLINE + date);
        job2.setJarByClass(CatePVStatistic.class);
        job2.setMapperClass(CatePVStatistic.CatePVStatisticMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(NullWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setNumReduceTasks(0);

        FileInputFormat.addInputPaths(job2, job1_output);
        FileOutputFormat.setOutputPath(job2, job2_outputPath);

        ControlledJob cJob2 = new ControlledJob(job2.getConfiguration());
        cJob2.setJob(job2);

        //-----[job4] 訂單對應部分的 page view
        Job job4 = new Job(conf, OrderStatisticAll.class.getSimpleName() + UNDERLINE + date);
        job4.setJarByClass(OrderStatisticAll.class);
        job4.setMapperClass(OrderStatisticAll.OrderStatisticAllMapper.class);
        job4.setReducerClass(OrderStatisticAll.OrderStatisticAllReducer.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(NullWritable.class);
        job4.setNumReduceTasks(1);

        FileInputFormat.addInputPaths(job4, input);
        FileOutputFormat.setOutputPath(job4, job4_outputPath);

        ControlledJob cJob4 = new ControlledJob(job4.getConfiguration());
        cJob4.setJob(job4);

        //-----[執行]
        int isCompletion = controlJobProcess("jobControl", cJob1, cJob2, cJob4);
        System.exit(isCompletion);
    }
}
