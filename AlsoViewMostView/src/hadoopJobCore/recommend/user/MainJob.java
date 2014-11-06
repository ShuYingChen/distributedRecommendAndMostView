package hadoopJobCore.recommend.user;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * user recommend 主要 JOB 執行區
 * 將 4 個 job 合併起來
 * Created by Joan on 2014/7/10.
 */
public class MainJob {
    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();    //計時器
        String itemVectorInput = args[0];
        String userVectorInput = args[1];
        Path outputPath = new Path(args[2]);
        String firstDay = args[3];  //所有輸入資料的第一天
        //推薦數量
        int recommendCount = (args.length >= 5) ? Integer.parseInt(args[4]) : DisUserRecommendJob.ResultReducer.RECOMMENDATIONS_COUNT;
        int numReduceTasks = (args.length >= 6) ? Integer.parseInt(args[5]) : 24;    //reducer 數量

        Path itemVectorOutput = new Path("/tmp/DisUserRecommend/itemVectorMerge");
        Path userVectorOutput = new Path("/tmp/DisUserRecommend/userVectorMerge");
        Path twoMapperOutput = new Path("/tmp/DisUserRecommend/twoMapperTemp");

        Configuration conf = new Configuration();
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");   // 同一個job的task都在一個JVM執行:-1
        // 超過 5hr timeout
        conf.set("mapred.task.timeout", "18000000"); // ms (預設是 10 mins)
        conf.set("dfs.socket.timeout", "18000000");
        conf.set("dfs.datanode.socket.write.timeout", "18000000");
        conf.set("mapred.reduce.slowstart.completed.maps", "0.75");  // mapper 最小完成率，啟動 reducer
        conf.set("mapred.reduce.parallel.copies", "10");  // reducer 平行複製的個數
        conf.set("io.sort.mb", "200");  // 寫入硬碟前的緩衝區大小
        conf.set("io.sort.factor", "15");  // 一次合併的檔案數量
        conf.set("mapred.child.java.opts", "-Xmx1g -XX:+UseCompressedStrings -XX:-UseGCOverheadLimit -XX:+UseConcMarkSweepGC");
        conf.setInt("mapred.userlog.retain.hours", 72);
        conf.setInt(DisUserRecommendJob.ResultReducer.RECOMMENDATIONS_PER_USER, recommendCount);
        conf.setStrings(DisUserDataVector.UserVectorReducer.FIRST_DAY, firstDay);

        FileSystem fs = FileSystem.get(conf);
        Job userVectorJob = getJob(conf, "UserVector_merge", DisUserDataVector.class, 30, Text.class, Text.class, Text.class, Text.class);
        Job itemVectorJob = getJob(conf, "ItemVector_merge", DisItemVector.class, 30, Text.class, Text.class, Text.class, Text.class);

        Job twoMapperJob = getJob(conf, "UserRecommendResult_preprocessing", TempRecommend.class, 150, Text.class, Text.class, Text.class, Text.class);
        conf.set("mapred.compress.map.output", "true"); // 壓縮 mapper output
        Job resultJob = getJob(conf, "DisUserRecommendJob", DisUserRecommendJob.class, numReduceTasks, Text.class, Text.class, Text.class, Text.class);

        ControlledJob controlledJob_userVectorJob = getControlledJob(userVectorJob, fs, userVectorInput, userVectorOutput);
        ControlledJob controlledJob_itemVectorJob = getControlledJob(itemVectorJob, fs, itemVectorInput, itemVectorOutput);
        ControlledJob controlledJob_twoMapperJob = getControlledJob(twoMapperJob, fs, TempRecommend.class, userVectorOutput, itemVectorOutput, twoMapperOutput);
        ControlledJob controlledJob_resultJob = getControlledJob(resultJob, fs, DisUserRecommendJob.class, userVectorOutput, twoMapperOutput, outputPath);

        JobControl jobControl = new JobControl("DisUserRecommendJobControl");
        jobControl.addJob(controlledJob_userVectorJob);
        jobControl.addJob(controlledJob_itemVectorJob);
        jobControl.addJob(controlledJob_twoMapperJob);
        jobControl.addJob(controlledJob_resultJob);

        controlledJob_twoMapperJob.addDependingJob(controlledJob_userVectorJob);
        controlledJob_twoMapperJob.addDependingJob(controlledJob_itemVectorJob);
        controlledJob_resultJob.addDependingJob(controlledJob_twoMapperJob);
        int isCompletion = jobProcess(jobControl);
        long totalTime = System.currentTimeMillis() - start;
        System.out.println("[total time] " + (totalTime / (60 * 60 * 1000)) % 24 + " hr " + (totalTime / (60 * 1000)) % 60 + " min " + (totalTime / 1000) % 60 + " sec");
        System.exit(isCompletion);
    }

    private static int jobProcess(JobControl jobControl) {
        Thread thread = new Thread(jobControl);
        thread.start();
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

    private static ControlledJob getControlledJob(Job job, FileSystem fs, String input, Path output) throws IOException {
        fs.delete(output, true);
        ControlledJob controlledJob = new ControlledJob(job.getConfiguration());
        controlledJob.setJob(job);
        FileInputFormat.addInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        FileOutputFormat.setCompressOutput(job, true);  //將 reducer 的輸出做壓縮
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);    //壓縮檔格式為 .gz
        return controlledJob;
    }

    private static ControlledJob getControlledJob(Job job, FileSystem fs, Class<?> className, Path input1, Path input2, Path output) throws IOException {
        fs.delete(output, true);
        ControlledJob controlledJob = new ControlledJob(job.getConfiguration());
        controlledJob.setJob(job);

        String subClassName;
        for (Class<?> subClass : className.getClasses()) {
            subClassName = subClass.getSimpleName();
            if (subClassName.contains("Mapper1")) {
                MultipleInputs.addInputPath(job, input1, TextInputFormat.class, (Class<? extends Mapper>) subClass);
            } else if (subClassName.contains("Mapper2")) {
                MultipleInputs.addInputPath(job, input2, TextInputFormat.class, (Class<? extends Mapper>) subClass);
            }
        }

        FileOutputFormat.setOutputPath(job, output);
        return controlledJob;
    }

    private static Job getJob(Configuration conf, String jobName, Class<?> className, int numReduceTasks,
                              Class<?> mapOutputKey, Class<?> mapOutputValue, Class<?> outputKey, Class<?> outputValue) throws IOException {
        Job job = new Job(conf, jobName);
        job.setJarByClass(MainJob.class);
        String subClassName;
        for (Class<?> subClass : className.getClasses()) {
            subClassName = subClass.getSimpleName();
            if (subClassName.contains("Mapper")) {
                job.setMapperClass((Class<? extends Mapper>) subClass);
            } else if (subClassName.contains("Combiner")) {
                job.setCombinerClass((Class<? extends Reducer>) subClass);
            } else if (subClassName.contains("Reducer")) {
                job.setReducerClass((Class<? extends Reducer>) subClass);
            }
        }
        job.setMapOutputKeyClass(mapOutputKey);
        job.setMapOutputValueClass(mapOutputValue);
        job.setOutputKeyClass(outputKey);
        job.setOutputValueClass(outputValue);
        job.setNumReduceTasks(numReduceTasks);

        return job;
    }
}
