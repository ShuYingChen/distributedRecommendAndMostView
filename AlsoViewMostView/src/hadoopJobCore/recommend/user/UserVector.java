package hadoopJobCore.recommend.user;

import jobUtil.Category;
import jobUtil.Item;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static jobUtil.Tool.*;

/**
 * 使用者向量前處理 per day
 * 輸入的資料需要以天為單位 - 一天算一份
 * Created by Joan on 2014/3/28.
 */
public class UserVector {

    /**
     * data input access log
     * Mar  6 00:00:01 ec1web14 apache2: 1.160.127.2 - - [06/Mar/2014:00:00:01 +0800] "GET /emon/v2/record.htm?%7B%22Uid%22%3A%22591d692e6e222f382e028b4c4b273b67b632b6f5%22%2C%22Site%22%3A%2224h%22%2C%22Page%22%3A%22prod%22%2C%22Action%22%3Aaddcart%2C%22Prod%22%3A%5B%22DCAO08-A80924960-000%22%5D%7D&callback=jQuery171018247749733238028_1394035196140&_=1394035205093 HTTP/1.1" 200 26 "http://shopping.pchome.com.tw/DCAO08-A80924960" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB7.5; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET4.0C)"
     */
    public static class LogUserDataMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final String view_pattern = "/emon/item.htm";
        private final String addCart_pattern = "/emon/v2/record.htm";
        private Pattern pattern;
        private Matcher matcher;
        private String userId = "", itemId = "";
        private HashMap<String, String> uid2eid;    // (uid, eid)
        public final static String mappingFilePath = "/tmp/DisUserRecommend/uid2eid";
        Text user = new Text(), info = new Text();

        protected void setup(Context context) throws IOException {
            uid2eid = getUid2eid(FileSystem.get(context.getConfiguration()), new Path(mappingFilePath));
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            StringBuilder outputValue = new StringBuilder();
            if (!bot.matcher(data).find()) {    // 不是爬蟲機器人
                if (data.contains(view_pattern)) {
                    getRecord(data, view_pattern);
                    outputValue.append(VIEW).append(UNDERLINE).append(itemId);
                } else if (data.contains(addCart_pattern)) {
                    getRecord(data, addCart_pattern);
                    outputValue.append(CART).append(UNDERLINE).append(itemId);
                }

                if (!userId.isEmpty() && outputValue.length() > 0) {  // 都有值才會輸出
                    user.set(userId);
                    info.set(outputValue.toString());
                    context.write(user, info);
                }
            }
        }

        private void getRecord(String log, String patterns) {
            String temp;
            if (log.contains(patterns)) {
                pattern = Pattern.compile(patterns + "[^\"]+");
                matcher = pattern.matcher(log);
                if (matcher.find()) {
                    log = URLDecoder.decode(matcher.group());//解 URL 編碼
                    userId = getUserId(log);
                    itemId = getItemId(log);
                    if (!(userId.isEmpty() || itemId.isEmpty())) {
                        temp = uid2eid.get(userId);
                        if (temp != null)
                            userId = EID + COLON + temp;
                        else
                            userId = UID + COLON + userId;
                    }
                }
            }
            //System.out.println("uid = " + userId + " pid = " + itemId);
        }

        private String getUserId(String url) {
            String uid = "";
            pattern = Pattern.compile("[uU]id[^,]+");
            matcher = pattern.matcher(url);
            if (matcher.find()) {
                uid = matcher.group();  //uid=08fb23fd7d9193aaa0f378bd9944b47f77689992
                pattern = Pattern.compile("\\w{40}");
                matcher = pattern.matcher(uid.substring(3));
                uid = matcher.find() ? matcher.group() : "";
            }
            return uid;
        }

        private String getItemId(String url) {
            String pid = "";
            pattern = Pattern.compile("pid[^,]+|Prod[^\\]]+");
            matcher = pattern.matcher(url);
            if (matcher.find()) {
                pattern = Pattern.compile("\\w{6}-\\w{9}");
                matcher = pattern.matcher(matcher.group());
                pid = matcher.find() ? matcher.group() : "";
            }
            return pid;
        }

        private HashMap<String, String> getUid2eid(FileSystem fs, Path path) throws IOException {
            HashMap<String, String> temp = new HashMap<String, String>();
            FileStatus[] srcFiles = fs.listStatus(path);    //回傳當下資料夾的所有檔案路徑
            for (FileStatus srcFile : srcFiles) {
                // srcFiles 裡面有好幾個 hadoop 結果檔 (part-r-xxxx)
                if (fs.isFile(srcFile.getPath()) && srcFile.getPath().getName().contains("part-")) {
                    FSDataInputStream in = fs.open(srcFile.getPath());    //開始讀取資料串流
                    LineReader lr = new LineReader(in);
                    Text line_content = new Text();
                    String[] array;
                    while (lr.readLine(line_content) > 0) {
                        array = StringUtils.split(line_content.toString(), TAB);
                        temp.put(array[0], array[1]);
                    }
                    in.close();
                }
            }
            return temp;
        }
    }

    /**
     * 處理購物清單的資料
     * 這裡的使用者 ID 為 eid
     * input format → 20121201000004-12	af3b897838c633ff6e1efbf075cd4a94643aca5d	DJAD3J-A57761725-000	DJAD3J	DJAD
     */
    public static class BuyListUserDataMapper extends Mapper<LongWritable, Text, Text, Text> {
        public static final String COMPUTE_PARA = "compute_para"; //統計的當天
        public static String COMPUTE_DATE = "";

        Text outputKey = new Text();
        Text outputValue = new Text();

        protected void setup(Context context) {
            COMPUTE_DATE = context.getConfiguration().get(COMPUTE_PARA);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            String buy_info_array[] = StringUtils.split(value.toString(), TAB);
            String buy_date = buy_info_array[0].substring(0, 8);
            String userId, item;
            int index;
            if (COMPUTE_DATE.equals(buy_date)) {   //是否與計算同天
                userId = EID + COLON + buy_info_array[1];
                index = buy_info_array[2].lastIndexOf("-");
                item = buy_info_array[2].substring(0, index);
                // 資料格式類似於 buy_DEAX44-A66376796_20121216
                sb.append(BUY).append(UNDERLINE).append(item).append(UNDERLINE).append(buy_date);

                if (userId.length() > 0 && sb.length() > 0) {
                    outputKey.set(userId);
                    outputValue.set(sb.toString());
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    /**
     * 紀錄 uid 與 eid
     */
    public static class Uid2EidMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final String uid2eid_pattern = "/emon/uidmap.htm";
        private Pattern pattern = Pattern.compile("[ue]id=[^,]+");
        private Matcher matcher;
        Text outputKey = new Text();
        Text outputValue = new Text();
        boolean isBot, isHasPattern;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String values2String = value.toString();
            String uid = "", eid = "", temp;
            isBot = bot.matcher(values2String).find();
            isHasPattern = values2String.contains(uid2eid_pattern);
            int index;
            if (!isBot && isHasPattern) {    // 不是爬蟲機器人
                Pattern p = Pattern.compile("\"GET[^\"]+");
                Matcher m = p.matcher(values2String);
                if (m.find())
                    values2String = m.group();

                matcher = pattern.matcher(values2String);
                while (matcher.find()) {
                    temp = matcher.group();
                    index = temp.indexOf("=") + 1;
                    if (temp.contains("uid"))
                        uid = temp.substring(index);
                    if (temp.contains("eid"))
                        eid = temp.substring(index);
                }
            }

            if (!(uid.isEmpty() || eid.isEmpty())) { //兩者不為空才輸出
                outputKey.set(uid);
                outputValue.set(eid);
                context.write(outputKey, outputValue);
            }
        }
    }

    /**
     * input key → 使用者ID
     * input value → buy_DEAX44-A66376796_20121216 or view_DEAX44-A66376796 or cart_DEAX44-A66376796
     * output key → 使用者ID   商品ID
     * output value → buy:count,view:count,cart:count,buy date
     */
    public static class UserDataReducer extends Reducer<Text, Text, Text, Text> {
        private HashMap<String, Item> itemsMap = new HashMap<String, Item>();   //(key, value) → (品編, 商品資訊)
        private final int ONE = 1;
        Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            itemsMap.clear();    //使用前先清空
            for (Text value : values) {
                addAll(value.toString());
            }

            StringBuilder sb_value = new StringBuilder();
            for (Item item : itemsMap.values()) {
                sb_value.append(item.item_no).append(TAB);
                sb_value.append(BUY).append(COLON).append(item.buyCount).append(COMMA);
                sb_value.append(VIEW).append(COLON).append(item.viewCount).append(COMMA);
                sb_value.append(CART).append(COLON).append(item.cartCount).append(COMMA);
                if (!item.startTime.isEmpty())
                    sb_value.append(item.startTime);
                else
                    sb_value.append(noTime);

                outputValue.set(sb_value.toString());
                context.write(key, outputValue);
                sb_value.setLength(0);
            }
        }

        private void addAll(String text) {
            String textArray[] = StringUtils.split(text, UNDERLINE);
            Category cat;
            if (textArray.length > 1) {
                Item item = itemsMap.get(textArray[1]);  // 類似於 CBAE9X-A49096282
                if (item == null)
                    item = new Item(textArray[1]);

                cat = Category.valueOf(textArray[0]);  // view or cart or buy
                //對相同品編的商品進行數量加總
                switch (cat) {
                    case buy:
                        item.setBuyCount(item.buyCount + ONE);
                        if (item.startTime.isEmpty())  //若 item 物件是由 view 或 cart 建的話，會沒有 startTime
                            item.setStartTime(textArray[2]);    //紀錄購買時間(start time)
                        break;
                    case cart:
                        item.setCartCount(item.cartCount + ONE);
                        break;
                    case view:
                        item.setViewCount(item.viewCount + ONE);
                        break;
                }
                itemsMap.put(item.item_no, item);
            }
        }
    }

    /**
     * [0] access log path, [1] 購物清單, [2] 輸出位置 [3] reducer 數量 [4] 資料來源的日期
     */
    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        int args_length = args.length;
        int numReduceTasks = (args_length >= 4) ? Integer.parseInt(args[3]) : 3;    // reducer 數量
        Path logData = new Path(args[0]);
        Path buyListData = new Path(args[1]);
        String outputData = args[2];
        Path outputPath = new Path(outputData);
        Pattern pattern = Pattern.compile("\\d{8}");
        String computeDay = "";
        if (args_length == 5) {
            computeDay = args[4];
            if (pattern.matcher(computeDay).find()) {
                System.out.println("[compute date] " + computeDay);
            } else {
                System.out.println("[compute date error] " + computeDay + " is not date format.");
                System.exit(0);
            }
        } else if (args_length == 4) {  //如果沒有輸入日期的話，從輸出路徑取得
            computeDay = outputData.substring(outputData.length() - 10).replace("/", "");
            System.out.println("[compute date] " + computeDay);
        }

        Configuration conf = new Configuration();
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");   // 同一個job的task都在一個JVM執行:-1
        conf.set("mapred.child.java.opts", "-Xmx768m -XX:+UseCompressedStrings -XX:-UseGCOverheadLimit");
        conf.setInt("mapred.userlog.retain.hours", 72);
        conf.setStrings(BuyListUserDataMapper.COMPUTE_PARA, computeDay);
        Path job2OutputPath = new Path(LogUserDataMapper.mappingFilePath);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);  //output path 使用前先刪除
        fs.delete(job2OutputPath, true);

        Job job = new Job(conf, "UserVector_" + computeDay);
        job.setJarByClass(UserVector.class);
        job.setReducerClass(UserDataReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(numReduceTasks);

        ControlledJob cJob1 = new ControlledJob(job.getConfiguration());
        cJob1.setJob(job);

        //使用 MultipleInputs 會取代 FileInputFormat.addInputPath() 和 job.setMapperClass()
        //若 mapper 相同，可以用 MultipleInputs.addInputPath(job, input, TextInputFormat.class) 加上 job.setMapperClass()
        MultipleInputs.addInputPath(job, logData, TextInputFormat.class, LogUserDataMapper.class);
        MultipleInputs.addInputPath(job, buyListData, TextInputFormat.class, BuyListUserDataMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        //----------------
        Job job2 = new Job(conf, "uid2eid_" + computeDay);
        job2.setJarByClass(UserVector.class);
        job2.setMapperClass(Uid2EidMapper.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);

        ControlledJob cJob2 = new ControlledJob(job2.getConfiguration());
        cJob2.setJob(job2);

        FileInputFormat.addInputPaths(job2, logData.toString());
        FileOutputFormat.setOutputPath(job2, job2OutputPath);

        int exitNum = controlJobProcess(cJob2, cJob1, "jobControl");
        long totalTime = System.currentTimeMillis() - start;
        System.out.println("[total time] " + (totalTime / 60000) % 60 + " min " + (totalTime / 1000) % 60 + " sec");
        System.exit(exitNum);
    }


}
