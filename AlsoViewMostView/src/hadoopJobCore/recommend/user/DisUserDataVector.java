package hadoopJobCore.recommend.user;

import jobUtil.Category;
import jobUtil.Item;
import jobUtil.TimeTool;
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
import java.util.HashMap;

import static jobUtil.Tool.*;

/**
 * 使用者向量值計算
 * 輸入資料可慢慢測試要計算幾天份
 * Created by Joan on 2014/3/28.
 */
public class DisUserDataVector {
    public static final String TIME = "time";

    /**
     * input data format likes below
     * 00c9ade79e49dc03ef57af6c13f851eca83ebb3f	DEBG2H-A70201693	buy:3,view:0,cart:0,20121207
     * 當使用者只有瀏覽沒有購買，其購買時間會顯示 noTime
     */
    public static class UserVectorMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text userID = new Text();
        Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] array = value.toString().split(TAB);
            userID.set(array[0]);
            outputValue.set(array[1] + TAB + array[2]);
            context.write(userID, outputValue);
        }
    }

    /**
     * per user
     * input key → 使用者ID
     * input value → 商品編號   商品資訊
     * output key → 使用者ID   商品編號
     * output value → 商品分數,平均購買週期(天數),最後一次購買日期
     */
    public static class UserVectorReducer extends Reducer<Text, Text, Text, Text> {
        public static final String FIRST_DAY = "firstDay";
        //各項目權重分數，以滿分 5 分來分配
        private final float view_weight = 1f;   //瀏覽
        private final float buy_weight = 2.5f;  //購買
        private final float cart_weight = 1.5f; //購物車
        String firstDay = "20140301";
        int totalView = 0, totalBuy = 0, totalCart = 0;
        Text outputValue = new Text();
        long diff_first_today_days;


        protected void setup(Context context) {
            firstDay = context.getConfiguration().get(FIRST_DAY, firstDay); //預設值再想過
            diff_first_today_days = TimeTool.numDateBetween(firstDay, today);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            clearAll(); //使用前先全清乾淨 且 歸零
            HashMap<String, Item> itemsMap = new HashMap<String, Item>();
            String[] arr;
            Item temp_item;
            for (Text value : values) {
                arr = StringUtils.split(value.toString(),TAB);
                temp_item = getItem(arr[0], getInfoMap(arr[1]), itemsMap);
                itemsMap.put(temp_item.item_no, temp_item);
            }

            float totalScore, viewScore, cartScore, buyScore;
            double avgDay;  //商品平均購買天數
            StringBuffer sb = new StringBuffer();
            for (Item item : itemsMap.values()) {
                //如果這個商品有買過，直接算平均天數；若沒買過，給他今天減第一天
                avgDay = (item.buyCount != 0) ? Math.ceil((float) TimeTool.numDateBetween(firstDay, item.endTime) / item.buyCount)
                        : diff_first_today_days;   //使之無條件進位
                //如果商品的最後購買日期 等於 輸入資料的第一天，avgDay 也會為 0
                avgDay = (avgDay != 0) ? avgDay : diff_first_today_days;

                viewScore = (totalView != 0) ? (float) item.viewCount / totalView : 0;
                cartScore = (totalCart != 0) ? (float) item.cartCount / totalCart : 0;
                buyScore = (totalBuy != 0) ? (float) item.buyCount / totalBuy : 0;

                totalScore = view_weight * viewScore + cart_weight * cartScore + buy_weight * buyScore;
                sb.append(item.item_no).append(TAB).append(df.format(totalScore)).append(TAB).append(avgDay).append(TAB).append(item.endTime);
                outputValue.set(sb.toString());
                context.write(key, outputValue);
                sb.setLength(0);
            }
        }

        private HashMap<String, String> getInfoMap(String info) {
            HashMap<String, String> info_map = new HashMap<String, String>(10);
            String[] data;
            for (String s : info.split(COMMA)) {
                if (s.contains(COLON)) {
                    data = s.split(COLON);
                    info_map.put(data[0], data[1]);
                } else
                    info_map.put(TIME, s);
            }
            return info_map;
        }

        private Item getItem(String itemID, HashMap<String, String> info, HashMap<String, Item> itemsMap) {
            Item item = itemsMap.get(itemID);
            if (item == null) {
                item = new Item(itemID);
                item.setEndTime(firstDay);
            }

            String info_value;
            int count = 0;
            Category cat;
            for (String info_name : info.keySet()) {
                cat = Category.valueOf(info_name);
                info_value = info.get(info_name);

                if (info_value.equals(noTime))
                    info_value = firstDay;   //預設值為輸入資料的第一天

                if (!cat.equals(Category.time))
                    count = Integer.parseInt(info_value);

                switch (cat) {
                    case buy:
                        item.setBuyCount(item.buyCount + count);
                        totalBuy += count;
                        break;
                    case cart:
                        item.setCartCount(item.cartCount + count);
                        totalCart += count;
                        break;
                    case view:
                        item.setViewCount(item.viewCount + count);
                        totalView += count;
                        break;
                    case time:
                        item.setEndTime(TimeTool.compare_date(item.endTime, info_value) == -1 ? item.endTime : info_value); //回傳 -1 表示前面的日期比較晚
                        break;
                }
            }

            return item;
        }

        private void clearAll() {
            totalView = 0;
            totalBuy = 0;
            totalCart = 0;
        }
    }

    public static void main(String[] args) throws Exception {
        String inputData = args[0];
        Path outputPath = new Path(args[1]);
        String firstDay = args[2];  //所有輸入資料的第一天
        int numReduceTasks = (args.length >= 4) ? Integer.parseInt(args[3]) : 3;    //reducer 數量
        boolean isOutputCompress = (args.length >= 5) && Boolean.parseBoolean(args[4]); //是否對輸出檔做壓縮

        Configuration conf = new Configuration();
        conf.set("mapred.task.timeout", "18000000"); // ms (預設是 10 mins)
        conf.set("dfs.socket.timeout", "18000000");
        conf.set("dfs.datanode.socket.write.timeout", "18000000");
        conf.set("mapred.child.java.opts", "-Xmx1024m -XX:+UseCompressedStrings -XX:-UseGCOverheadLimit");
        conf.setInt("mapred.userlog.retain.hours", 72);
        conf.setStrings(UserVectorReducer.FIRST_DAY, firstDay);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);

        Job job = new Job(conf, "UserVector_merge");
        job.setJarByClass(DisUserDataVector.class);
        job.setMapperClass(UserVectorMapper.class);
        job.setReducerClass(UserVectorReducer.class);
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
