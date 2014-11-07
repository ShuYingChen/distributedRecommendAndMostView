package jobCore.ec1web.itemPageViewHits;

import jobCore.ec1web.util.JsonKey;
import jobCore.ec1web.util.Uid2Eid;
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
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Pattern;

import static jobCore.ec1web.util.Tool.*;
import static jobUtil.GlobalTool.COMMA;

/**
 * parsing uid 對應的 eid
 * Created by Joan on 2014/10/9.
 */
public class UidEidMapping {
    public static class UidEidMappingMapper extends Mapper<LongWritable, Text, Text, Text> {
        Uid2Eid uid2Eid;
        Pattern pattern = Pattern.compile("record\\.htm?");
        Text output_key = new Text(), output_value = new Text();

        protected void setup(Context context) throws IOException, InterruptedException {
            uid2Eid = new Uid2Eid();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            if (!isBot(log) && uid2Eid.record_pattern.matcher(log).find()) {   //不是機器人 && 符合 uidmap 的 pattern
                uid2Eid.parseUidAndEid(log);
                output_key.set(uid2Eid.getUID());
                output_value.set(uid2Eid.getEID());
                context.write(output_key, output_value);
            }
            if (!isBot(log) && pattern.matcher(log).find()) {   //不是機器人 && 符合點擊的 pattern
                String request = getRequest(log);  //點擊的資料模型
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
                        String uid = (String) json.get(JsonKey.Uid.name());
                        String action = (String) json.get(JsonKey.Action.name());
                        output_key.set(uid);
                        output_value.set(action);
                        context.write(output_key, output_value);
                    }
                }
            }
        }
    }

    public static class UidEidMappingReducer extends Reducer<Text, Text, Text, Text> {
        Text output_value = new Text();
        StringBuilder sb = new StringBuilder();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> eid_set = new HashSet<String>();
            for (Text value : values) {
                eid_set.add(value.toString());
            }
            int i = 0;
            for (String eid : eid_set) {
                if (i > 0)
                    sb.append(COMMA);
                sb.append(eid);
                i++;
            }
            output_value.set(sb.toString());
            context.write(key, output_value);
            sb.setLength(0);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String input = args[0];
        String output = args[1];
        Path output_path = new Path(output);

        Configuration conf = new Configuration();
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");   // 同一個job的task都在一個JVM執行:-1
        conf.setStrings("io.sort.mb", "150");
        conf.set("mapred.task.timeout", "18000000"); // ms (預設是 10 mins)
        conf.set("dfs.socket.timeout", "18000000");
        conf.set("dfs.datanode.socket.write.timeout", "18000000");
        conf.set("mapred.child.java.opts", "-Xmx512m -XX:+UseCompressedStrings -XX:-UseGCOverheadLimit -XX:+UseConcMarkSweepGC");
        conf.setInt("mapred.userlog.retain.hours", 72);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(output_path, true);  // output path 使用前先刪除

        Job job = new Job(conf, UidEidMapping.class.getSimpleName());
        job.setJarByClass(PageViewHits.class);
        job.setMapperClass(UidEidMappingMapper.class);
        job.setReducerClass(UidEidMappingReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output_path);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
