package jobCore.ecapi.hydrograph;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static jobUtil.GlobalTool.*;

/**
 * 測試資料(on hadoop)：/temp_joan/analy_ecapi_log/data/test_file.txt
 * 分析 ecapi access log
 * 準備水位圖所需的數據資料
 * 統計 1)商品 2)非商品 3)total
 * per min
 * 個別機器統計
 * <p/>
 * 預計：key→<商品, 非商品, total> value→機器:統計量...
 * Created by Joan on 2014/8/20.
 */
public class ecapiAnalysisLog {

    public static class ecapiAnalysisLogMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text output_key = new Text(), output_value = new Text();
        Pattern product_pattern = Pattern.compile("pchome.com.tw/.*prod/([^/]+/)?\\w{6}-\\w{9}");
        Matcher product_matcher;
        static final String ONE = "1", ZERO = "0", TIME_START = "00", TIME_END = "59";

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            String machine = getMachine(StringUtils.split(log, " "));
            output_key.set(getOutputKey(getTime(log)));
            product_matcher = product_pattern.matcher(log);
            // ecapi1:1,0 → 機器名稱:商品數量,非商品數量
            if (product_matcher.find()) {
                output_value.set(machine + COLON + ONE + COMMA + ZERO);
            } else {
                output_value.set(machine + COLON + ZERO + COMMA + ONE);
            }

            context.write(output_key, output_value);
        }

        private String getOutputKey(String time) {
            StringBuilder sb = new StringBuilder();
            String temp = time.substring(0, time.lastIndexOf(COLON) + 1);
            sb.append(temp).append(TIME_START).append(UNDERLINE).append(temp).append(TIME_END);

            return sb.toString();
        }

        private String getMachine(String[] array) {
            return array[3];
        }

        private String getTime(String log) {
            return log.substring(0, 15);
        }
    }

    public static class ecapiAnalysisLogCombiner extends Reducer<Text, Text, Text, Text> {
        Text output_value = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TObjectIntHashMap<String> productCounter = new TObjectIntHashMap<String>();
            TObjectIntHashMap<String> non_productCounter = new TObjectIntHashMap<String>();
            String[] array, count_array;
            String machine;
            int pro_count, non_pro_count;
            for (Text info : values) {
                array = StringUtils.split(info.toString(), COLON);
                machine = array[0];
                count_array = StringUtils.split(array[1], COMMA);
                pro_count = Integer.parseInt(count_array[0]);
                non_pro_count = Integer.parseInt(count_array[1]);
                productCounter.adjustOrPutValue(machine, pro_count, pro_count);
                non_productCounter.adjustOrPutValue(machine, non_pro_count, non_pro_count);
            }

            for (String hostname : productCounter.keySet()) {
                output_value.set(hostname + COLON + productCounter.get(hostname) + COMMA + non_productCounter.get(hostname));
                context.write(key, output_value);
            }
        }
    }

    public static class ecapiAnalysisLogReducer extends Reducer<Text, Text, Text, Text> {
        Text output_value = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TObjectIntHashMap<String> productCounter = new TObjectIntHashMap<String>();
            TObjectIntHashMap<String> non_productCounter = new TObjectIntHashMap<String>();
            String[] array, count_array;
            String machine;
            int pro_count, non_pro_count;
            for (Text info : values) {
                array = StringUtils.split(info.toString(), COLON);
                machine = array[0];
                count_array = StringUtils.split(array[1], COMMA);
                pro_count = Integer.parseInt(count_array[0]);
                non_pro_count = Integer.parseInt(count_array[1]);
                productCounter.adjustOrPutValue(machine, pro_count, pro_count);
                non_productCounter.adjustOrPutValue(machine, non_pro_count, non_pro_count);
            }

            StringBuilder sb = new StringBuilder();
            int index = 0;
            for (String hostname : productCounter.keySet()) {
                if (index > 0)
                    sb.append(TAB);
                sb.append(hostname).append(COLON);
                sb.append(productCounter.get(hostname)).append(COMMA).append(non_productCounter.get(hostname));
                index++;
            }

            if (sb.length() > 0) {
                output_value.set(sb.toString());
                context.write(key, output_value);
            }
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String input = args[0];
        String output = args[1];
        Path outputPath = new Path(output);
        int reducerNum = (args.length >= 3) ? Integer.parseInt(args[2]) : 1;

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

        Job job = new Job(conf, ecapiAnalysisLog.class.getSimpleName() + UNDERLINE + getJobDate(output));
        job.setJarByClass(ecapiAnalysisLog.class);
        job.setMapperClass(ecapiAnalysisLogMapper.class);
        job.setCombinerClass(ecapiAnalysisLogCombiner.class);
        job.setReducerClass(ecapiAnalysisLogReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(reducerNum);

        FileInputFormat.addInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
