package hadoopJobCore.recommend.item;

import jobUtil.combineSmallFile.CustomCombineFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static jobUtil.Tool.*;

/**
 * item recommend 主要 JOB 執行區
 * 將 2 個 JOB 合併起來
 * <p/>
 * [0] input
 * [1] output
 * [2] reducer number : default 30
 * [3] recommend count : default 30
 * [4] filterCount : 過濾次數. default 0
 * <p/>
 * Created by Joan on 2014/7/11.
 */
public class MainJob {
    public static void main(String[] args) throws Exception {
        int arg_len = args.length;
        long start = System.currentTimeMillis();    //計時器
        String inputData = args[0];
        String userVector_outputPath = UserVector.USER_VECTOR_OUTPUT;  // JOB_1 的輸出路徑
        String itemVector_outputPath = args[1];    // JOB_2 的輸出路徑
        Path userVector_outputDataPath = new Path(userVector_outputPath);
        Path itemVector_outputDataPath = new Path(itemVector_outputPath);
        int job2_reducerNum = (arg_len >= 3) ? Integer.parseInt(args[2]) : 30;
        int recommendCount = (arg_len >= 4) ? Integer.parseInt(args[3]) : ItemVector.ItemVectorReducer.RECOMMENDATIONS_COUNT;
        int filterCount = (arg_len >= 5) ? Integer.parseInt(args[4]) : 0; // 略過次數的商品

        int months = inputDates(inputData);
        int job1_reducerNum = 144;

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
        conf.set(MODE, "item"); // 推薦模式

        FileSystem fs = FileSystem.get(conf);
        fs.delete(userVector_outputDataPath, true);  // JOB_1 output path 使用前先刪除
        fs.delete(itemVector_outputDataPath, true);  // JOB_2 output path 使用前先刪除

        //-------[userVector]---------
        Job job = new Job(conf, "DisItemRecommend-userVector");
        job.setJarByClass(UserVector.class);
        job.setMapperClass(UserVector.UserVectorMapper.class);
        job.setReducerClass(UserVector.UserVectorReducer.class);
        job.setCombinerClass(UserVector.UserVectorCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(CustomCombineFileInputFormat.class);    //測試合併輸入檔
        if (months > 3)
            job.setNumReduceTasks(50 * months);
        else
            job.setNumReduceTasks(job1_reducerNum);

        ControlledJob cJob1 = new ControlledJob(job.getConfiguration());
        cJob1.setJob(job);

        FileInputFormat.addInputPaths(job, inputData);
        FileOutputFormat.setOutputPath(job, userVector_outputDataPath);

        //-------[itemVector]---------
        conf.setInt(ItemVector.ItemVectorMapper.ITEM_PER_MONTH, months);
        conf.setInt(ItemVector.ItemVectorReducer.RECOMMENDATIONS_PER_ITEM, recommendCount);

        //對 map 輸出的內容進行壓縮
        conf.set("mapred.compress.map.output", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "0.75");  // mapper 最小完成率，啟動 reducer
        conf.setInt(ItemVector.ItemVectorReducer.FILTER_COUNT, filterCount);

        Job job2 = new Job(conf, "DisItemRecommend_" + getYesterday());
        job2.setJarByClass(ItemVector.class);
        job2.setMapperClass(ItemVector.ItemVectorMapper.class);
        job2.setReducerClass(ItemVector.ItemVectorReducer.class);
        job2.setCombinerClass(ItemVector.ItemVectorCombiner.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(job2_reducerNum);

        ControlledJob cJob2 = new ControlledJob(job2.getConfiguration());
        cJob2.setJob(job2);

        FileInputFormat.addInputPaths(job2, userVector_outputPath);
        FileOutputFormat.setOutputPath(job2, itemVector_outputDataPath);

        int isCompletion = controlJobProcess(cJob1, cJob2, "jobControl");
        long totalTime = System.currentTimeMillis() - start;
        System.out.println("[total time] " + (totalTime / 60000) % 60 + " min " + (totalTime / 1000) % 60 + " sec");
        System.exit(isCompletion);
    }
}
