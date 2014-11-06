package hadoopJobCore.mostView.store;

import jobUtil.Tool;
import jobUtil.combineSmallFile.CustomCombineFileInputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static jobUtil.Tool.MODE;
import static jobUtil.Tool.UNDERLINE;

/**
 * most view - 館
 * 統計最多人瀏覽的商品
 * Created by Joan on 2014/11/4.
 */
public class StoreMostView {
    /**
     * input → 00c9ade79e49dc03ef57af6c13f851eca83ebb3f    DEBG2H_DEBG2H-A70201693
     * output → DEBG2H    DEBG2H-A70201693
     */
    public static class StoreMostViewMapper extends Mapper<Text, Text, Text, Text> {
        final String item_format = "\\w{6}_\\w{6}-\\w{9}";
        Text output_key = new Text();
        Text output_value = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String[] data_array;
            if (data.matches(item_format)) {
                data_array = StringUtils.split(data, Tool.UNDERLINE);
                output_key.set(data_array[0]);
                output_value.set(data_array[1]);
                context.write(output_key, output_value);
            }
        }
    }

    /**
     * input → DEBG2H    DEBG2H-A70201693
     * output → DEBG2H DEBG2H-A70201693:5,...
     */
    public static class StoreMostViewReducer extends Reducer<Text, Text, Text, Text> {
        public static final String TOP_COUNT_PER_STORE_ITEM = "topCountPerStoreItem";
        public static int TOP = 10;
        Text output_value = new Text();

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            TOP = conf.getInt(TOP_COUNT_PER_STORE_ITEM, TOP);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> item_count_map = new HashMap<String, Integer>();
            String item;
            Integer count;
            for (Text value : values) {
                item = value.toString();
                count = item_count_map.get(item);
                count = (count == null) ? 1 : (count + 1);
                item_count_map.put(item, count);
            }
            List<Map.Entry<String, Integer>> list = Tool.sortMap(item_count_map);
            StringBuilder sb = new StringBuilder();
            int limit = Math.min(list.size(), TOP);
            for (int i = 0; i < limit; i++) {
                if (i > 0)
                    sb.append(Tool.COMMA);
                sb.append(list.get(i).getKey());
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
        int reducerNum = (args.length >= 3) ? Integer.parseInt(args[2]) : 1;
        int mostViewCount = (args.length >= 4) ? Integer.parseInt(args[3]) : StoreMostViewReducer.TOP;
        String exeDate = (args.length >= 5) ? args[4] : Tool.getYesterday();

        Path output_path = new Path(output);
        Configuration conf = new Configuration();
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");   // 同一個job的task都在一個JVM執行:-1
        conf.setStrings("io.sort.mb", "150");
        conf.set("mapred.task.timeout", "18000000"); // ms (預設是 10 mins)
        conf.set("dfs.socket.timeout", "18000000");
        conf.set("dfs.datanode.socket.write.timeout", "18000000");
        conf.set("mapred.child.java.opts", "-Xmx512m -XX:+UseCompressedStrings -XX:-UseGCOverheadLimit -XX:+UseConcMarkSweepGC");
        conf.setInt("mapred.userlog.retain.hours", 72);
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 67108864);    //設置與 block size 相同
        conf.set(MODE, "store"); // 資料輸入模式
        conf.setInt(StoreMostViewReducer.TOP_COUNT_PER_STORE_ITEM, mostViewCount);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(output_path, true);

        Job job = new Job(conf);
        job.setJarByClass(StoreMostView.class);
        job.setJobName(StoreMostView.class.getSimpleName() + UNDERLINE + exeDate);
        job.setMapperClass(StoreMostViewMapper.class);
        job.setReducerClass(StoreMostViewReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(CustomCombineFileInputFormat.class);    //合併輸入檔
        job.setNumReduceTasks(reducerNum);

        FileInputFormat.addInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output_path);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
