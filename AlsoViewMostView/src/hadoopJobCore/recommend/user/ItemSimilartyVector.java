package hadoopJobCore.recommend.user;

import gnu.trove.map.hash.TObjectIntHashMap;
import hadoopJobCore.recommend.item.UserVector;
import jobUtil.RecommendItem;
import jobUtil.RecommendItemComparator;
import jobUtil.combineSmallFile.CustomCombineFileInputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.regex.Pattern;

import static jobUtil.Tool.*;

/**
 * 用以計算商品的相似向量
 * Created by Joan on 2014/7/10.
 */
public class ItemSimilartyVector {
    public static final String USER_OUTPUT = "/tmp/DisUserRecommend/userItems";

    /**
     * input → DEBG2H-A70201693,DJAD3J-A57761725
     * output → DEBG2H-A70201693	AHAE5K-A70973596:2,A61808768:3,AHAE4D-A70354986:1
     */
    public static class ItemSimilartyVectorMapper extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, TObjectIntHashMap<String>> outputData = new HashMap<String, TObjectIntHashMap<String>>();
        Text output_key = new Text(), output_value = new Text();
        public static final int OUTPUT_COUNTER = 1000;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            HashSet<String> itemSet = new HashSet<String>();
            Collections.addAll(itemSet, StringUtils.split(value.toString(), COMMA));
            if (itemSet.size() < OUTPUT_COUNTER) {
                TObjectIntHashMap<String> pair;
                for (String firstItem : itemSet) {
                    pair = outputData.get(firstItem);
                    if (pair == null)
                        pair = new TObjectIntHashMap<String>();

                    for (String secondItem : itemSet) {
                        if (firstItem.equals(secondItem))
                            continue;
                        pair.adjustOrPutValue(secondItem, 1, 1);
                    }
                    outputData.put(firstItem, pair);
                }
            }

            if (outputData.size() > OUTPUT_COUNTER) {
                writeData(context);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            writeData(context);
        }

        private void writeData(Context context) throws IOException, InterruptedException {
            HashMap<String, TObjectIntHashMap<String>> copy = (HashMap<String, TObjectIntHashMap<String>>) outputData.clone();
            outputData.clear(); // 清空
            StringBuilder sb = new StringBuilder();
            TObjectIntHashMap<String> pair;
            int index;
            for (String firstItem : copy.keySet()) {
                pair = copy.get(firstItem);
                output_key.set(firstItem);
                index = 0;
                for (String secondItem : pair.keySet()) {
                    if (index > 0)
                        sb.append(COMMA);
                    sb.append(secondItem).append(COLON).append(pair.get(secondItem));
                    index++;
                }
                if (sb.length() > 0) {
                    output_value.set(sb.toString());
                    context.write(output_key, output_value);
                }
                sb.setLength(0);
            }
        }
    }

    public static class ItemSimilartyVectorCombiner extends Reducer<Text, Text, Text, Text> {
        Text output_value = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TObjectIntHashMap<String> trovePair = new TObjectIntHashMap<String>();
            int count;
            String[] tempArray;
            for (Text value : values) {
                for (String itemCount : StringUtils.split(value.toString(), COMMA)) {
                    tempArray = StringUtils.split(itemCount, COLON);
                    count = Integer.parseInt(tempArray[1]);
                    trovePair.adjustOrPutValue(tempArray[0], count, count);
                }
            }

            StringBuilder sb = new StringBuilder();
            int index = 0;
            for (String item : trovePair.keySet()) {
                if (index > 0)
                    sb.append(COMMA);
                sb.append(item).append(COLON).append(trovePair.get(item));
                index++;
            }
            if (sb.length() > 0) {
                output_value.set(sb.toString());
                context.write(key, output_value);
            }
        }
    }

    public static class ItemSimilartyVectorReducer extends Reducer<Text, Text, Text, Text> {
        Text output_value = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TObjectIntHashMap<String> pair = new TObjectIntHashMap<String>();
            String store = key.toString().substring(0, 5);
            int count;
            String[] tempArray;
            for (Text value : values) {
                for (String itemCount : StringUtils.split(value.toString(), COMMA)) {
                    tempArray = StringUtils.split(itemCount, COLON);
                    count = Integer.parseInt(tempArray[1]);
                    pair.adjustOrPutValue(tempArray[0], count, count);
                }
            }

            Vector<RecommendItem> recommendItemVector = getRecommendItemVector(store, pair);    //取得排序過後的推薦結果
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

        private Vector<RecommendItem> getRecommendItemVector(String store, TObjectIntHashMap<String> pair) {
            int score;
            String item_store;
            Vector<RecommendItem> v = new Vector<RecommendItem>(pair.size());
            for (String item : pair.keySet()) {
                item_store = item.substring(0, 5);
                score = pair.get(item);
                if (item_store.equals(store))
                    score = score << 1; // 左移1位等同於乘以2
                v.add(new RecommendItem(item, score));
            }
            pair.clear();
            pair.trimToSize();
            Collections.sort(v, new RecommendItemComparator());

            return v;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();    //計時器
        String inputData = args[0];
        String userVector_outputPath = USER_OUTPUT;  // JOB_1 的輸出路徑
        String ItemSimilartyVector_outputPath = args[1];    // JOB_2 的輸出路徑
        Path userVector_outputDataPath = new Path(userVector_outputPath);
        Path ItemSimilartyVector_outputDataPath = new Path(ItemSimilartyVector_outputPath);
        int reducerNum = (args.length >= 3) ? Integer.parseInt(args[2]) : 3;

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
            computeDay = ItemSimilartyVector_outputPath.substring(ItemSimilartyVector_outputPath.length() - 10).replace("/", "");
            System.out.println("[compute date] " + computeDay);
        }

        Configuration conf = new Configuration();
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");   // 同一個job的task都在一個JVM執行:-1
        conf.set("mapred.task.timeout", "18000000"); // ms (預設是 10 mins)
        conf.set("dfs.socket.timeout", "18000000");
        conf.set("dfs.datanode.socket.write.timeout", "18000000");
        conf.set("mapred.child.java.opts", "-Xmx896m -XX:+UseCompressedStrings -XX:-UseGCOverheadLimit");
        conf.setInt("mapred.userlog.retain.hours", 72);
        // hadoop 1.2.1 parameter name
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 67108864);    //設置與 block size 相同

        FileSystem fs = FileSystem.get(conf);
        fs.delete(userVector_outputDataPath, true);  // JOB_1 output path 使用前先刪除
        fs.delete(ItemSimilartyVector_outputDataPath, true);  // JOB_2 output path 使用前先刪除

        //-------[userVector]---------
        Job job1 = new Job(conf, "DisUserRecommend-viewPrepare");
        job1.setJarByClass(hadoopJobCore.recommend.item.UserVector.class);
        job1.setMapperClass(UserVector.UserVectorMapper.class);
        job1.setReducerClass(UserVector.UserVectorReducer.class);
        job1.setCombinerClass(UserVector.UserVectorCombiner.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);
        job1.setInputFormatClass(CustomCombineFileInputFormat.class);    //測試合併輸入檔
        job1.setNumReduceTasks(reducerNum);

        ControlledJob cJob1 = new ControlledJob(job1.getConfiguration());
        cJob1.setJob(job1);

        FileInputFormat.addInputPaths(job1, inputData);
        FileOutputFormat.setOutputPath(job1, userVector_outputDataPath);

        //-------[itemVector]---------
        Job job2 = new Job(conf, "DisUserRecommend-itemVector_" + computeDay);
        job2.setJarByClass(ItemSimilartyVector.class);
        job2.setMapperClass(ItemSimilartyVectorMapper.class);
        job2.setCombinerClass(ItemSimilartyVectorCombiner.class);
        job2.setReducerClass(ItemSimilartyVectorReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(reducerNum);

        ControlledJob cJob2 = new ControlledJob(job2.getConfiguration());
        cJob2.setJob(job2);

        FileInputFormat.addInputPaths(job2, userVector_outputPath);
        FileOutputFormat.setOutputPath(job2, ItemSimilartyVector_outputDataPath);

        int isCompletion = controlJobProcess(cJob1, cJob2, "jobControl");
        long totalTime = System.currentTimeMillis() - start;
        System.out.println("[total time] " + (totalTime / 60000) % 60 + " min " + (totalTime / 1000) % 60 + " sec");
        System.exit(isCompletion);
    }
}
