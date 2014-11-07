package jobCore.ec1web.itemPageViewHits;

import jobUtil.GlobalTool;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static jobCore.ec1web.util.Tool.sortMap;
import static jobUtil.GlobalTool.*;

/**
 * 對當月的週、月份做排序
 * 以 StorePageRankByDate 的結果當作 input
 * Created by Joan on 2014/10/14.
 */
public class StorePageRankByWeekMonth {
    public static class StorePageRankByWeekMonthMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text output_key = new Text(), output_value = new Text();
        int line_index = 0;
        String[] field_arr;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String[] data_arr;
            if (line_index > 0) {
                data_arr = data.split(TAB);
                String SR_CNAME, ST_NO, count, field;
                for (int i = 2; i < field_arr.length; i++) {
                    if (i <= data_arr.length) {
                        SR_CNAME = data_arr[i - 2] != null ? data_arr[i - 2] : "";
                        ST_NO = data_arr[i - 1] != null ? data_arr[i - 1] : "";
                        count = data_arr[i] != null ? data_arr[i] : "";
                        if (!SR_CNAME.isEmpty() && !ST_NO.isEmpty()) {
                            field = field_arr[i - 2].substring(0, field_arr[i - 2].indexOf("-"));
                            output_key.set(field);
                            output_value.set(SR_CNAME + TAB + ST_NO + TAB + count);
                            context.write(output_key, output_value);
                            System.out.println(output_key.toString() + "\t" + output_value.toString());
                        }
                    }
                    i += 2;
                }
            } else {
                field_arr = data.split(TAB);
                line_index++;
            }
        }
    }

    public static class StorePageRankByWeekMonthReducer extends Reducer<Text, Text, Text, NullWritable> {
        HashMap<String, List<Map.Entry<String, Integer>>> result = new HashMap<String, List<Map.Entry<String, Integer>>>();
        HashMap<String, Integer> All_store_count = new HashMap<String, Integer>();
        Text output_key = new Text();
        NullWritable nullWritable = NullWritable.get();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String site, store_no, store_name, data, store_count_key;
            site = key.toString();
            HashMap<String, Integer> store_count = new HashMap<String, Integer>();
            Integer count, all_count, data_count;
            String[] data_arr;
            for (Text value : values) {
                data = value.toString();
                data_arr = data.split(TAB);
                store_name = data_arr[0];
                store_no = data_arr[1];
                data_count = Integer.parseInt(data_arr[2]);
                store_count_key = store_name + TAB + store_no;
                count = store_count.get(store_count_key);
                count = (count == null) ? 0 : count;
                store_count.put(store_count_key, count + data_count);

                if (site.equals("ALL")) {   //全部的就直接用全部加總
                    all_count = All_store_count.get(store_count_key);
                    all_count = (all_count == null) ? 0 : all_count;
                    All_store_count.put(store_count_key, all_count + data_count);   //所有站台的館頁
                }
            }
            result.put(site, sortMap(store_count)); // site -> (store, count)
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            String[] site_arr = {"ecshop", "24h", "books", "globaltw", "globalen", "mobile", "ecssl", "ecvip"};
            List<Map.Entry<String, Integer>> All_list = sortMap(All_store_count);
            StringBuilder sb = new StringBuilder();
            //組欄位名稱
            sb.append("ALL-SR_CNAME").append(TAB).append("ALL-ST_NO").append(TAB).append("count");
            for (String site : site_arr) {
                sb.append(TAB).append(site).append("-SR_CNAME").append(TAB).append(site).append("-ST_NO").append(TAB).append("count");
            }
            output_key.set(sb.toString());
            context.write(output_key, nullWritable);
            sb.setLength(0);
            List<Map.Entry<String, Integer>> list;
            String map_key;
            for (int i = 0; i < All_list.size(); i++) {
                if (All_list.get(i).getValue() > 1) {
                    map_key = All_list.get(i).getKey();
                    sb.append(map_key).append(TAB).append(All_list.get(i).getValue());
                    for (String site : site_arr) {
                        list = result.get(site);
                        if (list != null && list.size() > i) {
                            map_key = list.get(i).getKey();
                            sb.append(TAB).append(map_key).append(TAB).append(list.get(i).getValue());
                        } else {
                            sb.append(TAB).append("").append(TAB).append("").append(TAB).append("");
                        }
                    }
                    output_key.set(sb.toString());
                    context.write(output_key, nullWritable);
                    sb.setLength(0);
                }
            }
        }
    }

    static String getInputList(String input, int thisWeek) {
        StringBuilder sb = new StringBuilder();
        ArrayList<String> list = weekOfMonth.get(thisWeek);
        String year, month;
        for (int i = 0; i < list.size(); i++) {
            year = list.get(i).substring(0, 4);
            month = list.get(i).substring(4, 6);
            if (i > 0)
                sb.append(COMMA);
            sb.append(input).append(year).append(File.separator).append(month).append(File.separator).append(list.get(i));
            if (list.get(i).equals(exeDate))
                break;
        }

        return sb.toString();
    }

    static String getInputList(String input) {
        String[][] seasonArray = {{"01", "02", "03"}, {"04", "05", "06"}, {"07", "08", "09"}, {"10", "11", "12"}};
        String[] sea_arr;
        StringBuilder sb = new StringBuilder();
        String year = exeDate.substring(0, 4);
        String month = exeDate.substring(4, 6);
        int month_int = (Integer.parseInt(month) - 1) / 3;
        sea_arr = seasonArray[month_int];
        for (int i = 0; i < sea_arr.length; i++) {
            if (i > 0)
                sb.append(COMMA);
            sb.append(input).append(year).append(File.separator).append(sea_arr[i]);
            if (sea_arr[i].equals(month))
                break;
        }

        return sb.toString();
    }

    static String getWeek(int thisWeek) {
        String[] week_array = {"", "first", "second", "third", "fourth", "fifth", "sixth"};
        return week_array[thisWeek];
    }

    static String getSeason() {
        String[] season_array = {"first_season", "second_season", "third_season", "fourth_season"};
        String month = exeDate.substring(4, 6);
        int month_int = (Integer.parseInt(month) - 1) / 3;
        return season_array[month_int];
    }

    public static void main(String[] args) throws ParseException, IOException, ClassNotFoundException, InterruptedException {
        String yesterday = GlobalTool.getYesterday();
        String input = args[0];
        String output = args[1];
        String dateMode = args.length >= 3 ? args[2].toLowerCase() : "day";  //week or (month or season)
        String processDate = args.length >= 4 ? args[3] : yesterday;    //預設值是前一天
        prepareThisMonthData(processDate);
        String date = processDate.substring(0, 6);  //取 年月
        String week = getWeek(thisWeek);    //將數字轉換成對應的英文
        String prepareInput = input;
        if (dateMode.equals("week")) {  //如果是算週
            //input 直接輸入路徑 /project/analy/data/StorePageRank/day/
            prepareInput = (input.contains(COMMA)) ? input : getInputList(input, thisWeek);   //只取當週的當 input
            output = output + File.separator + week;
        } else if (dateMode.equals("season")) {
            //input 直接輸入路徑 /project/analy/data/StorePageRank/month/
            prepareInput = (input.contains(COMMA)) ? input : getInputList(input);
            output = output + File.separator + getSeason();
        }

        Path outputPath = new Path(output);

        Configuration conf = new Configuration();
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");   // 同一個job的task都在一個JVM執行:-1
        conf.setStrings("io.sort.mb", "150");
        conf.set("mapred.task.timeout", "18000000"); // ms (預設是 10 mins)
        conf.set("dfs.socket.timeout", "18000000");
        conf.set("dfs.datanode.socket.write.timeout", "18000000");
        conf.set("mapred.child.java.opts", "-Xmx512m -XX:+UseCompressedStrings -XX:-UseGCOverheadLimit -XX:+UseConcMarkSweepGC");
        conf.setInt("mapred.userlog.retain.hours", 72);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);  // output path 使用前先刪除

        Job job = new Job(conf);
        String job_name;
        if (dateMode.equals("day")) {
            job_name = StorePageRankByDate.class.getSimpleName() + UNDERLINE + getJobDate(output);
            job.setJarByClass(StorePageRankByDate.class);
            job.setMapperClass(StorePageRankByDate.StorePageRankByDateMapper.class);
            job.setReducerClass(StorePageRankByDate.StorePageRankByDateReducer.class);
        } else {
            job_name = StorePageRankByWeekMonth.class.getSimpleName() + UNDERLINE + date + UNDERLINE + dateMode;
            job.setJarByClass(StorePageRankByWeekMonth.class);
            job.setMapperClass(StorePageRankByWeekMonthMapper.class);
            job.setReducerClass(StorePageRankByWeekMonthReducer.class);
        }
        job.setJobName(job_name);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPaths(job, prepareInput);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
