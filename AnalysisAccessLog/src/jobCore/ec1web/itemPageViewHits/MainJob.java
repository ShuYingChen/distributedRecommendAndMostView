package jobCore.ec1web.itemPageViewHits;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static jobUtil.GlobalTool.UNDERLINE;
import static jobUtil.GlobalTool.getJobDate;

/**
 * 組合所有 JOB
 * Created by Joan on 2014/9/26.
 */
public class MainJob {
    public static void main(String[] args) throws IOException, InterruptedException {
        String input = args[0];
        String output = args[1];
        String job1_output = output + File.separator + "ALL";
        String job2_output = output + File.separator + "CatePVStatistic";
        String job3_output = output + File.separator + "StorePageRank";
        String job4_output = output + File.separator + "itemOrderStatistic";
        String job5_output = output + File.separator + "StoreOrderStatistic";
        Path job1_outputPath = new Path(job1_output);
        Path job2_outputPath = new Path(job2_output);
        Path job3_outputPath = new Path(job3_output);
        Path job4_outputPath = new Path(job4_output);
        Path job5_outputPath = new Path(job5_output);
        int top = (args.length >= 3) ? Integer.parseInt(args[2]) : 100; //預設 top 100
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
        fs.delete(job3_outputPath, true);  // output path 使用前先刪除
        fs.delete(job4_outputPath, true);  // output path 使用前先刪除
        fs.delete(job5_outputPath, true);  // output path 使用前先刪除

        //-----[job1] 全部資料
        Job job = new Job(conf, PageViewHits.class.getSimpleName() + UNDERLINE + date);
        job.setJarByClass(PageViewHits.class);
        job.setMapperClass(PageViewHits.PageViewHitsMapper.class);
        job.setReducerClass(PageViewHits.PageViewHitsReducer.class);
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

        //-----[job3] 各站台館頁排行榜
        conf.setInt(StorePageRank.StorePageRankReducer.rankTop, top);
        Job job3 = new Job(conf, StorePageRank.class.getSimpleName() + UNDERLINE + date);
        job3.setJarByClass(StorePageRank.class);
        job3.setMapperClass(StorePageRank.StorePageRankMapper.class);
        job3.setReducerClass(StorePageRank.StorePageRankReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(NullWritable.class);
        job3.setNumReduceTasks(1);

        FileInputFormat.addInputPaths(job3, input);
        FileOutputFormat.setOutputPath(job3, job3_outputPath);

        ControlledJob cJob3 = new ControlledJob(job3.getConfiguration());
        cJob3.setJob(job3);

        //-----[job4] 單品訂單統計
//        Job job4 = new Job(conf, OrderStatistic.class.getSimpleName() + UNDERLINE + date);
//        job4.setJarByClass(OrderStatistic.class);
//        job4.setMapperClass(OrderStatistic.OrderStatisticMapper.class);
//        job4.setCombinerClass(OrderStatistic.OrderStatisticCombiner.class);
//        job4.setReducerClass(OrderStatistic.OrderStatisticReducer.class);
//        job4.setMapOutputKeyClass(Text.class);
//        job4.setMapOutputValueClass(IntWritable.class);
//        job4.setOutputKeyClass(Text.class);
//        job4.setOutputValueClass(NullWritable.class);
//        job4.setNumReduceTasks(1);
//
//        ControlledJob cJob4 = new ControlledJob(job4.getConfiguration());
//        cJob4.setJob(job4);
//
//        FileInputFormat.addInputPaths(job4, input);
//        FileOutputFormat.setOutputPath(job4, job4_outputPath);
//
//        //-----[job5] 館訂單統計
//        Job job5 = new Job(conf, StoreOrderStatistic.class.getSimpleName() + UNDERLINE + date);
//        job5.setJarByClass(StoreOrderStatisticTest.class);
//        job5.setCombinerClass(StoreOrderStatisticTest.StoreOrderStatisticReducer.class);
//        job5.setMapOutputKeyClass(Text.class);
//        job5.setMapOutputValueClass(Text.class);
//        job5.setOutputKeyClass(Text.class);
//        job5.setOutputValueClass(Text.class);
//        job5.setNumReduceTasks(1);
//
//        ControlledJob cJob5 = new ControlledJob(job5.getConfiguration());
//        cJob5.setJob(job5);
//
//        MultipleInputs.addInputPath(job5, job3_outputPath, TextInputFormat.class, StoreOrderStatisticTest.StoreOrderStatisticIIMapper.class);
//        MultipleInputs.addInputPath(job5, job4_outputPath, TextInputFormat.class, StoreOrderStatisticTest.StoreOrderStatisticMapper.class);
//        FileOutputFormat.setOutputPath(job5, job5_outputPath);

        //-------
        int isCompletion = controlJobProcess("jobControl", cJob1, cJob2, cJob3);
        System.exit(isCompletion);
    }

    public static int controlJobProcess(String name, ControlledJob... jobs) throws InterruptedException {
        JobControl jobControl = new JobControl(name);
        ArrayList<ControlledJob> list = new ArrayList<ControlledJob>();
        Collections.addAll(list, jobs);
        for (int i = 0; i < list.size(); i++) {
            jobControl.addJob(list.get(i));
            if (i > 0) {
                list.get(i).addDependingJob(list.get(i - 1));
            }
        }
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
