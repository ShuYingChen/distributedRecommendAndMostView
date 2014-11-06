package hadoopJobCore.recommend.user;

import gnu.trove.map.hash.TObjectIntHashMap;
import jobUtil.RecommendItem;
import jobUtil.RecommendItemComparator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.Vector;

import static jobUtil.Tool.*;

/**
 * 商品向量值計算
 * 輸入資料可慢慢測試要計算幾天份
 * Created by Joan on 2014/3/28.
 */

public class DisItemVector {

    /**
     * input format → AAAE2Y-A70106149	AFAE4E-A59731010:1,DMAG1J-A64984485:1
     * output format : key → AAAE2Y-A70106149 ; value → AFAE4E-A59731010:1,DMAG1J-A64984485:1...
     */
    public static class ItemVectorMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text first_item = new Text(), item_count = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] dataArray = value.toString().split(TAB);
            first_item.set(dataArray[0]);
            item_count.set(dataArray[1]);
            context.write(first_item, item_count);
        }
    }

    public static class ItemVectorReducer extends Reducer<Text, Text, Text, Text> {
        Text output_value = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TObjectIntHashMap<String> sum_map = new TObjectIntHashMap<String>();
            String[] item_count_arr;
            int count;
            for (Text value : values) {
                for (String item_count : StringUtils.split(value.toString(), COMMA)) {
                    item_count_arr = StringUtils.split(item_count, COLON);
                    count = Integer.parseInt(item_count_arr[1]);
                    sum_map.adjustOrPutValue(item_count_arr[0], count, count);
                }
            }
            sum_map.trimToSize();

            Vector<RecommendItem> recommendItemVector = getSortItemVector(sum_map);
            RecommendItem recommendItem;
            StringBuilder sb = new StringBuilder();
            for (int index = 0; index < recommendItemVector.size(); index++) {
                recommendItem = recommendItemVector.get(index);
                if (index > 0)
                    sb.append(COMMA);
                sb.append(recommendItem.item).append(COLON).append(recommendItem.score);
            }

            if (sb.length() > 0) {
                output_value.set(sb.toString());
                context.write(key, output_value);
            }
        }

        private Vector<RecommendItem> getSortItemVector(TObjectIntHashMap<String> map) {
            int vector_limit = 1000;    // 限制個數
            Vector<RecommendItem> v = new Vector<RecommendItem>(map.size());
            int count;
            for (String item : map.keySet()) {
                count = map.get(item);
                if (count > 1)  // 大於 1 次才取 (不包含)
                    v.add(new RecommendItem(item, count));
            }

            Collections.sort(v, new RecommendItemComparator());
            if (v.size() > vector_limit) {
                v.setSize(vector_limit);
                v.trimToSize();
            }

            return v;
        }
    }

    public static void main(String[] args) throws Exception {
        int numReduceTasks = (args.length >= 3) ? Integer.parseInt(args[2]) : 3;    // reducer 數量
        String inputData = args[0];
        Path outputPath = new Path(args[1]);
        boolean isOutputCompress = (args.length >= 4) && Boolean.parseBoolean(args[3]); //是否對輸出檔做壓縮

        Configuration conf = new Configuration();
        conf.set("mapred.task.timeout", "18000000"); // ms (預設是 10 mins)
        conf.set("dfs.socket.timeout", "18000000");
        conf.set("dfs.datanode.socket.write.timeout", "18000000");
        conf.set("mapred.child.java.opts", "-Xmx1024m -XX:+UseCompressedStrings -XX:-UseGCOverheadLimit");
        conf.setInt("mapred.userlog.retain.hours", 72);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);

        Job job = new Job(conf, "ItemVector_merge");
        job.setJarByClass(DisItemVector.class);
        job.setMapperClass(ItemVectorMapper.class);
        job.setReducerClass(ItemVectorReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(numReduceTasks);

        FileInputFormat.addInputPaths(job, inputData);
        FileOutputFormat.setOutputPath(job, outputPath);
        if (isOutputCompress) {
            FileOutputFormat.setCompressOutput(job, true);  //將 reducer 的輸出做壓縮
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);    //壓縮檔格式為 .gz
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
