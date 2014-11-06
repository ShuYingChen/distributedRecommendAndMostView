package hadoopJobCore.recommend.store;

import jobUtil.combineSmallFile.CustomCombineFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static jobUtil.Tool.*;

/**
 * 分散式館推薦的 main job
 * 主要有三個 job
 * <p/>
 * [0] input
 * [1] output
 * [2] reducer number : default 9
 * [3] recommend count : default 30
 * [4] TopCountPerStoreItem : 每個館商品取前幾筆. default 10
 * <p/>
 * Created by Joan on 2014/8/12.
 */
public class MainJob {
    public static void main(String[] args) throws Exception {
        int arg_len = args.length;
        long start = System.currentTimeMillis();    //計時器
        String inputData = args[0];
        String job1_output = StoreUserVector.STORE_USER_VECTOR_OUTPUT;
        String job2_output = StoreItemVector.STORE_ITEM_VECTOR_OUTPUT;
        String job3_output = args[1];
        int job3_reducerNum = (arg_len >= 3) ? Integer.parseInt(args[2]) : 9;
        int recommendCount = (arg_len >= 4) ? Integer.parseInt(args[3]) : StoreRecommend.StoreRecommendReducer.RECOMMENDATIONS_COUNT;
        int TopCountPerStoreItem = (arg_len >= 5) ? Integer.parseInt(args[4]) : StoreItemVector.StoreItemVectorReducer.TOP; // 略過次數的商品

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
        conf.set(MODE, "store"); // 推薦模式

        FileSystem fs = FileSystem.get(conf);
        fs.delete(job1_output_path, true);  // JOB_1 output path 使用前先刪除
        fs.delete(job2_output_path, true);  // JOB_2 output path 使用前先刪除
        fs.delete(job3_output_path, true);  // JOB_3 output path 使用前先刪除

        //-----[job1]-----
        Job job1 = new Job(conf, "DisStoreRecommend-userVector");
        job1.setJarByClass(StoreUserVector.class);
        job1.setMapperClass(StoreUserVector.StoreUserVectorMapper.class);
        job1.setCombinerClass(StoreUserVector.StoreUserVectorCombiner.class);
        job1.setReducerClass(StoreUserVector.StoreUserVectorReducer.class);
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
        conf.setInt(StoreItemVector.StoreItemVectorMapper.ITEM_PER_MONTH, months);
        conf.setInt(StoreItemVector.StoreItemVectorReducer.TOP_COUNT_PER_STORE_ITEM, TopCountPerStoreItem);
        Job job2 = new Job(conf, "DisStoreRecommend-itemVector");
        job2.setJarByClass(StoreItemVector.class);
        job2.setMapperClass(StoreItemVector.StoreItemVectorMapper.class);
        job2.setCombinerClass(StoreItemVector.StoreItemVectorCombiner.class);
        job2.setReducerClass(StoreItemVector.StoreItemVectorReducer.class);
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
        conf.setInt(StoreRecommend.StoreRecommendReducer.RECOMMENDATIONS_PER_ITEM, recommendCount);
        Job job3 = new Job(conf, "DisStoreRecommend_" + getYesterday());
        job3.setJarByClass(StoreRecommend.class);
        job3.setMapperClass(StoreRecommend.StoreRecommendMapper.class);
        job3.setCombinerClass(StoreRecommend.StoreRecommendCombiner.class);
        job3.setReducerClass(StoreRecommend.StoreRecommendReducer.class);
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

    public static int controlJobProcess(String name, ControlledJob job1, ControlledJob job2, ControlledJob job3) throws InterruptedException {
        JobControl jobControl = new JobControl(name);
        jobControl.addJob(job1);
        jobControl.addJob(job2);
        jobControl.addJob(job3);
        job2.addDependingJob(job1);   //job1 做完才呼叫 job2 執行
        job3.addDependingJob(job2);   //job2 做完才呼叫 job3 執行
        Thread theController = new Thread(jobControl);
        theController.start();
        while (true) {
            if (jobControl.allFinished()) {
                System.out.println(jobControl.getSuccessfulJobList());
                jobControl.stop();
                return 0;
            }

            if (jobControl.getFailedJobList().size() > 0) {
                System.out.println(jobControl.getFailedJobList());
                jobControl.stop();
                return 1;
            }
        }
    }
}
