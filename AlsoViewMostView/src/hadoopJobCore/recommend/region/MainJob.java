package hadoopJobCore.recommend.region;

import jobUtil.combineSmallFile.CustomCombineFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static hadoopJobCore.recommend.store.MainJob.controlJobProcess;
import static jobUtil.Tool.*;

/**
 * 分散式區推薦的 main job
 * 主要有三個 job
 * <p/>
 * [0] input
 * [1] output
 * [2] reducer number : default 9
 * [3] recommend count : default 30
 * [4] TopCountPerRegionItem : 每個區商品取前幾筆. default 50
 * * Created by Joan on 2014/9/3.
 */
public class MainJob {
    public static void main(String[] args) throws Exception {
        int arg_len = args.length;
        long start = System.currentTimeMillis();    //計時器
        String inputData = args[0];
        String job1_output = RegionUserVector.REGION_USER_VECTOR_OUTPUT;
        String job2_output = RegionItemVector.REGION_ITEM_VECTOR_OUTPUT;
        String job3_output = args[1];

        int job3_reducerNum = (arg_len >= 3) ? Integer.parseInt(args[2]) : 9;
        int recommendCount = (arg_len >= 4) ? Integer.parseInt(args[3]) : RegionRecommend.RegionRecommendReducer.RECOMMENDATIONS_COUNT;
        int TopCountPerRegionItem = (arg_len >= 5) ? Integer.parseInt(args[4]) : RegionItemVector.RegionItemVectorReducer.TOP;

        Path job1_output_path = new Path(job1_output);
        Path job2_output_path = new Path(job2_output);
        Path job3_output_path = new Path(job3_output);
        int months = inputDates(inputData);
        int job1_reducerNum = 144;
        int job2_reducerNum = 27;

        Configuration conf = new Configuration();
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");   // 同一個job的task都在一個JVM執行:-1
        conf.set("mapred.reduce.parallel.copies", "10");   // Reduce copy 資料的進程數量
        conf.setStrings("io.sort.mb", "250");
        conf.setStrings("io.sort.factor", "15");
        conf.set("mapred.task.timeout", "18000000"); // ms (預設是 10 mins)
        conf.set("dfs.socket.timeout", "18000000");
        conf.set("dfs.datanode.socket.write.timeout", "18000000");
        conf.set("mapred.child.java.opts", "-Xmx1g -XX:+UseCompressedStrings -XX:-UseGCOverheadLimit -XX:+UseConcMarkSweepGC");
        conf.setInt("mapred.userlog.retain.hours", 72);
        // hadoop 1.2.1 parameter name
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 67108864);    //設置與 block size 相同
        conf.set(MODE, "region"); // 推薦模式

        FileSystem fs = FileSystem.get(conf);
        fs.delete(job1_output_path, true);  // JOB_1 output path 使用前先刪除
        fs.delete(job2_output_path, true);  // JOB_2 output path 使用前先刪除
        fs.delete(job3_output_path, true);  // JOB_3 output path 使用前先刪除

        //-----[job1]-----
        Job job1 = new Job(conf, "DisRegionRecommend-userVector");
        job1.setJarByClass(RegionUserVector.class);
        job1.setMapperClass(RegionUserVector.RegionUserVectorMapper.class);
        job1.setCombinerClass(RegionUserVector.RegionUserVectorCombiner.class);
        job1.setReducerClass(RegionUserVector.RegionUserVectorReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(CustomCombineFileInputFormat.class);    //合併輸入檔
        job1.setNumReduceTasks(job1_reducerNum);

        ControlledJob cJob1 = new ControlledJob(job1.getConfiguration());
        cJob1.setJob(job1);

        FileInputFormat.addInputPaths(job1, inputData);
        FileOutputFormat.setOutputPath(job1, job1_output_path);

        //-----[job2]-----
        conf.set("mapred.compress.map.output", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "0.75");  // mapper 最小完成率，啟動 reducer
        conf.setInt(RegionItemVector.RegionItemVectorMapper.ITEM_PER_MONTH, months);
        conf.setInt(RegionItemVector.RegionItemVectorReducer.TOP_COUNT_PER_REGION_ITEM, TopCountPerRegionItem);
        Job job2 = new Job(conf, "DisRegionRecommend-itemVector");
        job2.setJarByClass(RegionItemVector.class);
        job2.setMapperClass(RegionItemVector.RegionItemVectorMapper.class);
        job2.setCombinerClass(RegionItemVector.RegionItemVectorCombiner.class);
        job2.setReducerClass(RegionItemVector.RegionItemVectorReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(job2_reducerNum);

        ControlledJob cJob2 = new ControlledJob(job2.getConfiguration());
        cJob2.setJob(job2);

        FileInputFormat.addInputPaths(job2, job1_output);
        FileOutputFormat.setOutputPath(job2, job2_output_path);

        //-----[job3]-----
        conf.set("mapred.reduce.slowstart.completed.maps", "0.3");  // mapper 最小完成率，啟動 reducer
        conf.setInt(RegionRecommend.RegionRecommendReducer.RECOMMENDATIONS_PER_ITEM, recommendCount);
        Job job3 = new Job(conf, "DisRegionRecommend_" + getYesterday());
        job3.setJarByClass(RegionRecommend.class);
        job3.setMapperClass(RegionRecommend.RegionRecommendMapper.class);
        job3.setCombinerClass(RegionRecommend.RegionRecommendCombiner.class);
        job3.setReducerClass(RegionRecommend.RegionRecommendReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setNumReduceTasks(job3_reducerNum);

        ControlledJob cJob3 = new ControlledJob(job3.getConfiguration());
        cJob3.setJob(job3);

        FileInputFormat.addInputPaths(job3, job2_output);
        FileOutputFormat.setOutputPath(job3, job3_output_path);
        //-----[end]-----
        int isCompletion = controlJobProcess("jobControl", cJob1, cJob2, cJob3);
        long totalTime = System.currentTimeMillis() - start;
        System.out.println("[total time] " + (totalTime / 60000) % 60 + " min " + (totalTime / 1000) % 60 + " sec");
        System.exit(isCompletion);
    }
}
