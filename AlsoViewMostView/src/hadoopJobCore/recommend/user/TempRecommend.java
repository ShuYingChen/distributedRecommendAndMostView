package hadoopJobCore.recommend.user;

import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

import static jobUtil.Tool.*;

/**
 * 採用兩個 mapper
 * 一個輸入 user vector
 * 一個輸入 item vector
 * 使兩個 mapper 的 output 相同 (item,[user:score or item vector])
 * Created by Joan on 2014/4/1.
 */
public class TempRecommend {

    /**
     * user vector
     * (使用者ID,商品編號,分數,購買週期(天),最後購買日)
     * input format : 000055bb8de2eff4f62e1cd2878517eeb0fa124c	DAAM1A-159609322	2.5	18.0	20121219
     * output format :  key → DAAM1A-159609322 ; value → 000055bb8de2eff4f62e1cd2878517eeb0fa124c_2.5
     */
    public static class UserChainMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        static Text user_score = new Text(), itemNo = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] array = value.toString().split(TAB);
            itemNo.set(array[1]);   // DAAM1A-159609322
            user_score.set(array[0] + UNDERLINE + array[2]);    // 000055bb8de2eff4f62e1cd2878517eeb0fa124c_2.5
            if (itemNo.getLength() > 0 && user_score.getLength() > 0)
                context.write(itemNo, user_score);
        }
    }

    /**
     * item vector
     * input format : AAAE2Y-A70106149	AFAE4E-A59731010:1,DMAG1J-A64984485:1
     * output format : key → AAAE2Y-A70106149 ; value → AFAE4E-A59731010:1,DMAG1J-A64984485:1
     */
    public static class ItemChainMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        static Text first_item = new Text(), second_item = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] array = value.toString().split(TAB);
            first_item.set(array[0]);  // AAAE2Y-A70106149
            second_item.set(array[1]);  // AFAE4E-A59731010:1,DMAG1J-A64984485:1
            context.write(first_item, second_item);
        }
    }

    /**
     * 預先整理 user mapper 的輸出
     */
    public static class TempRecommendCombiner extends Reducer<Text, Text, Text, Text> {
        static Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String text2string;
            TObjectFloatHashMap<String> user_vector = new TObjectFloatHashMap<String>();
            String[] user_score_arr;
            float score;
            for (Text value : values) {
                text2string = value.toString();
                if (text2string.contains(UNDERLINE)) {  //user 會有好幾組
                    for (String user_score : StringUtils.split(text2string, COMMA)) {
                        user_score_arr = StringUtils.split(user_score, UNDERLINE);
                        score = Float.parseFloat(user_score_arr[1]);
                        user_vector.adjustOrPutValue(user_score_arr[0], score, score);
                    }
                } else { // 理應 each item 只有一條才對
                    context.write(key, value);
                }
            }

            if (user_vector.size() > 0) {   // user vector output
                StringBuilder sb = new StringBuilder();
                int index = 0;
                for (String userId : user_vector.keySet()) {
                    if (index > 0)
                        sb.append(COMMA);
                    sb.append(userId).append(UNDERLINE).append(user_vector.get(userId));
                    index++;
                }

                if (sb.length() > 0) {
                    outputValue.set(sb.toString());
                    context.write(key, outputValue);
                }
            }
        }
    }

    /**
     * 測試使用 trove 看看是否可以減少記憶體使用量
     * output   uid:000055bb8de2eff4f62e1cd2878517eeb0fa124c    DAAM1A-159609322:2.5,DGALL7-A83825004:0.125...
     */
    public static class TempRecommendReducer extends Reducer<Text, Text, Text, Text> {
        final static int OUTPUT_COUNTER = 500;
        static int RECOMMENDATIONS_COUNT = 30;  //輸出的推薦數量
        static int LIMIT = RECOMMENDATIONS_COUNT;
        HashMap<String, HashMap<String, Float>> result = new HashMap<String, HashMap<String, Float>>();
        Text item_score = new Text(), userId = new Text();

        protected void setup(Context context) {
            RECOMMENDATIONS_COUNT = context.getConfiguration().getInt(DisUserRecommendJob.ResultReducer.RECOMMENDATIONS_PER_USER, RECOMMENDATIONS_COUNT);
            LIMIT = RECOMMENDATIONS_COUNT * 3;  //只取前幾名的相乘結果給下個job做相加
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TObjectIntHashMap<String> item_vector = new TObjectIntHashMap<String>();
            TObjectFloatHashMap<String> user_vector = new TObjectFloatHashMap<String>();
            String text2string;
            String[] item_count_arr, user_score_arr;
            int count;
            float score;
            for (Text value : values) {
                text2string = value.toString();
                if (text2string.contains(UNDERLINE)) {
                    for (String user_score : StringUtils.split(text2string, COMMA)) {
                        user_score_arr = StringUtils.split(user_score, UNDERLINE);
                        // uid:aaa863bff107f4b1cb78f993ae63f50d405de148_0.083
                        score = Float.parseFloat(user_score_arr[1]);
                        user_vector.adjustOrPutValue(user_score_arr[0], score, score);
                    }
                } else {
                    for (String item_count : StringUtils.split(text2string, COMMA)) {
                        item_count_arr = StringUtils.split(item_count, COLON);
                        count = Integer.parseInt(item_count_arr[1]);
                        item_vector.put(item_count_arr[0], count);
                    }
                }
            }

            float computeScore, userScore;
            Float itemScore;
            HashMap<String, Float> vector;
            for (String userID : user_vector.keySet()) { //每個ID對所有item
                vector = result.get(userID);
                if (vector == null)
                    vector = new HashMap<String, Float>();
                userScore = user_vector.get(userID);
                for (String item : item_vector.keySet()) {
                    computeScore = userScore * item_vector.get(item);
                    itemScore = vector.get(item);
                    if (itemScore == null)
                        itemScore = 0f;
                    vector.put(item, itemScore + computeScore);
                }
                result.put(userID, vector);

                if (result.size() > OUTPUT_COUNTER) {
                    writeOut(context);
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            writeOut(context);
        }

        private void writeOut(Context context) throws IOException, InterruptedException {
            HashMap<String, HashMap<String, Float>> copy = (HashMap<String, HashMap<String, Float>>) result.clone();
            result.clear();
            List<Map.Entry<String, Float>> list;
            Map.Entry<String, Float> entry;
            StringBuilder sb = new StringBuilder();
            for (String user : copy.keySet()) {
                //按分數由大到小排序
                list = new ArrayList<Map.Entry<String, Float>>(copy.get(user).entrySet());
                Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
                    public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {
                        return (int) ((o2.getValue() - o1.getValue()) * 1000);
                    }
                });

                int min = Math.min(LIMIT, list.size());
                list = list.subList(0, min);
                for (int index = 0; index < min; index++) {
                    entry = list.get(index);
                    if (index > 0)
                        sb.append(COMMA);
                    sb.append(entry.getKey()).append(COLON).append(df.format(entry.getValue()));
                }

                if (sb.length() > 0) {
                    userId.set(user);
                    item_score.set(sb.toString());
                    context.write(userId, item_score);
                }
                sb.setLength(0);
            }
        }
    }
}
