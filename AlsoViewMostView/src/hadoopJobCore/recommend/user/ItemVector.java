package hadoopJobCore.recommend.user;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Pattern;

import static jobUtil.Tool.*;

/**
 * 計算商品共現次數 by user - per day
 * Created by Joan on 2014/3/28.
 */
public class ItemVector {
    public static final IntWritable ONE = new IntWritable(1);
    /**
     * input format → 000055bb8de2eff4f62e1cd2878517eeb0fa124c	DAAM1A-159609322	buy:1,view:0,cart:0,20121219
     * 蒐集使用者對應的商品
     */
    public static class ItemCoocurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text userID = new Text();
        Text itemID = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(TAB);
            userID.set(data[0]);
            itemID.set(data[1]);
            context.write(userID, itemID);
        }
    }

    /**
     * input key → 使用者ID
     * input value → 商品集合
     * 蒐集同一個使用者的商品 pair
     * output → (item1  item2, 1)
     */
    public static class ItemCoocurrenceReducer extends Reducer<Text, Text, Text, IntWritable> {
        Text item_pair = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> itemSet = new HashSet<String>();
            for (Text value : values) {
                itemSet.add(value.toString());
            }

            for (String first_item : itemSet) {
                for (String second_item : itemSet) {
                    if (first_item.equals(second_item))
                        continue;

                    item_pair.set(first_item + TAB + second_item);
                    context.write(item_pair, ONE);
                }
            }
        }
    }

    /**
     * 蒐集商品共現次數
     * input format → item1 item2   count
     */
    public static class ItemCoocurrenceMapper2 extends Mapper<LongWritable, Text, Text, MapWritable> {
        Text first_item = new Text(), second_item = new Text();
        IntWritable count = new IntWritable();
        MapWritable item_count = new MapWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            item_count.clear();
            String[] dataArray = value.toString().split(TAB);
            first_item.set(dataArray[0]);
            second_item.set(dataArray[1]);
            count.set(Integer.parseInt(dataArray[2]));
            item_count.put(second_item, count);
            context.write(first_item, item_count);
        }
    }

    /**
     * 統計商品共現次數加總
     * output format → item1 item2   count
     */
    public static class ItemCoocurrenceReducer2 extends Reducer<Text, MapWritable, Text, Text> {
        IntWritable count = new IntWritable();
        Text output_value = new Text();
        StringBuilder sb = new StringBuilder();
        TObjectIntHashMap<String> sum_map = new TObjectIntHashMap<String>();

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            sum_map.clear();
            for (MapWritable value : values) {
                for (Writable mapWritable_key : value.keySet()) {
                    count = (IntWritable) value.get(mapWritable_key);
                    sum_map.adjustOrPutValue(mapWritable_key.toString(), count.get(), count.get());
                }
            }
            sum_map.trimToSize();

            int count = 0;
            for (String item : sum_map.keySet()) {
                if (count > 0)
                    sb.append(COMMA);
                sb.append(item).append(COLON).append(sum_map.get(item));
                count++;
            }

            if (sb.length() > 0) {
                output_value.set(sb.toString());
                context.write(key, output_value);
                sb.setLength(0);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        String inputData = args[0];
        String outputData = args[1];
        Path outputPath = new Path(outputData);
        int numReduceTasks = (args.length >= 3) ? Integer.parseInt(args[2]) : 3;    //reducer 數量
        Pattern pattern = Pattern.compile("\\d{8}");
        String computeDay = "";
        if (args.length == 4) {
            computeDay = args[3];
            if (pattern.matcher(computeDay).find()) {
                System.out.println("[compute date] " + computeDay);
            } else {
                System.out.println("[compute date error] " + computeDay + " is not date format.");
                System.exit(0);
            }
        } else if (args.length == 3) {  //如果沒有輸入日期的話，從輸出路徑取得
            computeDay = outputData.substring(outputData.length() - 10).replace("/", "");
            System.out.println("[compute date] " + computeDay);
        }

        String job1_outputPath = "/tmp/DisUserRecommend/temp_result";    // job1 的輸出位置

        Configuration conf = new Configuration();
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");   // 同一個job的task都在一個JVM執行:-1
        conf.set("mapred.child.java.opts", "-Xmx512m -XX:+UseCompressedStrings -XX:-UseGCOverheadLimit");
        conf.setInt("mapred.userlog.retain.hours", 72);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);
        fs.delete(new Path(job1_outputPath), true);
        //-----------------------Job1
        String job_name = "Co_occurrencePreprocess1_" + computeDay;
        Job job = new Job(conf, job_name);
        job.setJarByClass(ItemVector.class);
        job.setMapperClass(ItemCoocurrenceMapper.class);
        job.setReducerClass(ItemCoocurrenceReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(numReduceTasks);

        ControlledJob cJob1 = new ControlledJob(job.getConfiguration());
        cJob1.setJob(job);

        FileInputFormat.addInputPaths(job, inputData);
        FileOutputFormat.setOutputPath(job, new Path(job1_outputPath));
        //-----------------------Job2
        job_name = "Co_occurrencePreprocess2_" + computeDay;
        Job job2 = new Job(conf, job_name);
        job2.setJarByClass(ItemVector.class);
        job2.setMapperClass(ItemCoocurrenceMapper2.class);
        job2.setReducerClass(ItemCoocurrenceReducer2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(MapWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(numReduceTasks);

        ControlledJob cJob2 = new ControlledJob(job2.getConfiguration());
        cJob2.setJob(job2);

        FileInputFormat.addInputPaths(job2, job1_outputPath);
        FileOutputFormat.setOutputPath(job2, outputPath);

        int exitNum = controlJobProcess(cJob1, cJob2, "ItemVectorControl");
        long totalTime = System.currentTimeMillis() - start;
        System.out.println("[total time] " + (totalTime / 60000) % 60 + " min " + (totalTime / 1000) % 60 + " sec");
        System.exit(exitNum);
    }

}
