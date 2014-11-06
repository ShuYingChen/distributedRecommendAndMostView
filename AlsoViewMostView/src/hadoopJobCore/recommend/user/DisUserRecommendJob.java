package hadoopJobCore.recommend.user;

import gnu.trove.map.hash.TObjectFloatHashMap;
import jobUtil.TimeTool;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

import static jobUtil.Tool.*;

/**
 * 整理預乘積之後輸出最後結果
 * Created by Joan on 2014/4/8.
 */
public class DisUserRecommendJob {

    /**
     * input format : uid:000055bb8de2eff4f62e1cd2878517eeb0fa124c    DAAM1A-159609322:2.5,DGALL7-A83825004:0.125...
     * output format : key → uid:000055bb8de2eff4f62e1cd2878517eeb0fa124c ; value → AFAE4E-A59731010:0.2...
     */
    public static class ResultMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        Text item_score = new Text(), userID = new Text();
        HashMap<String, TObjectFloatHashMap<String>> result = new HashMap<String, TObjectFloatHashMap<String>>();
        final static int OUTPUT_COUNTER = 1000;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(TAB);
            String userId = data[0];
            TObjectFloatHashMap<String> vector = result.get(userId);
            if (vector == null)
                vector = new TObjectFloatHashMap<String>();
            String[] arr;
            float score;
            for (String item_score : StringUtils.split(data[1], COMMA)) {
                arr = StringUtils.split(item_score, COLON);
                score = Float.parseFloat(arr[1]);
                vector.adjustOrPutValue(arr[0], score, score);
            }
            result.put(userId, vector);

            if (result.size() > OUTPUT_COUNTER) {
                writeOut(context);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            writeOut(context);
        }

        private void writeOut(Context context) throws IOException, InterruptedException {
            HashMap<String, TObjectFloatHashMap<String>> copy = (HashMap<String, TObjectFloatHashMap<String>>) result.clone();
            result.clear();
            TObjectFloatHashMap<String> vector;
            StringBuilder sb = new StringBuilder();
            int index;
            for (String user : copy.keySet()) {
                vector = copy.get(user);
                index = 0;
                for (String item : vector.keySet()) {
                    if (index > 0)
                        sb.append(COMMA);
                    sb.append(item).append(COLON).append(df.format(vector.get(item)));
                    index++;
                }

                if (sb.length() > 0) {
                    userID.set(user);
                    item_score.set(sb.toString());
                    context.write(userID, item_score);
                }
                sb.setLength(0);
            }
        }
    }

    /**
     * 用來處理使用者有購買過的商品
     * 檢查是否有過購買週期
     * input format : 000055bb8de2eff4f62e1cd2878517eeb0fa124c	DAAM1A-159609322	2.5	18.0	20121219
     * output format : key → ff00d92e341030ad7316459405c01c6b907f48d6 ; value → DAAL14-169540417
     */
    public static class FilterResultMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        Text userID = new Text(), itemNo = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(TAB);
            userID.set(data[0]);
            float avgDay = Float.parseFloat(data[3]);  //平均購買天數
            String lastBuyDay = data[4];   //最後一次購買日
            //資料為 noTime 表示沒有買過，可以推薦使用者買
            //是否已達購買週期，true 表示可以推薦
            boolean isReady = lastBuyDay.equals(noTime) || (TimeTool.numDateBetween(lastBuyDay, today) > avgDay);
            if (!isReady) {
                itemNo.set(data[1]); // add
                context.write(userID, itemNo);
            }
        }
    }

    /**
     * 預先整理 mapper 的輸出結果，再傳給 reducer
     */
    public static class ResultCombiner extends Reducer<Text, Text, Text, Text> {
        Text output_value = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TObjectFloatHashMap<String> collection_map = new TObjectFloatHashMap<String>();
            StringBuilder sb = new StringBuilder();
            String value2String;
            float pre_score;
            String[] itemScore_arr;
            for (Text value : values) {
                value2String = value.toString();
                if (value2String.contains(COLON)) { // DAAM1A-159609322:2.5,DGALL7-A83825004:0.125...
                    for (String item_score : StringUtils.split(value2String, COMMA)) {
                        itemScore_arr = StringUtils.split(item_score, COLON);
                        pre_score = Float.parseFloat(itemScore_arr[1]);
                        collection_map.adjustOrPutValue(itemScore_arr[0], pre_score, pre_score);
                    }
                } else {
                    if (sb.length() > 0)
                        sb.append(COMMA);
                    sb.append(value2String);
                }
            }

            if (sb.length() > 0) {  // 要排除的 items
                output_value.set(sb.toString());
                context.write(key, output_value);
            }
            sb.setLength(0);

            int index = 0;
            for (String item : collection_map.keySet()) {
                if (index > 0)
                    sb.append(COMMA);
                sb.append(item).append(COLON).append(collection_map.get(item));
                index++;
            }

            if (sb.length() > 0) {
                output_value.set(sb.toString());
                context.write(key, output_value);
            }
        }
    }

    public static class ResultReducer extends Reducer<Text, Text, Text, Text> {
        public static final String RECOMMENDATIONS_PER_USER = "recommendationsPerUser";
        public static int RECOMMENDATIONS_COUNT = 30;  //輸出的推薦數量
        Text outputValue = new Text();

        protected void setup(Context context) {
            RECOMMENDATIONS_COUNT = context.getConfiguration().getInt(RECOMMENDATIONS_PER_USER, RECOMMENDATIONS_COUNT);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> filterSet = new HashSet<String>();
            HashMap<String, Float> temp_map = new HashMap<String, Float>();
            String value2string, item;
            Float score;
            String[] itemNo_score_arr;
            for (Text value : values) {
                value2string = value.toString();
                if (value2string.contains(COLON)) { // DAAM1A-159609322:2.5,DGALL7-A83825004:0.125...
                    for (String item_score : StringUtils.split(value2string, COMMA)) {
                        itemNo_score_arr = StringUtils.split(item_score, COLON);
                        item = itemNo_score_arr[0];
                        score = temp_map.get(item);
                        if (score == null)
                            score = 0f;
                        temp_map.put(item, Float.parseFloat(itemNo_score_arr[1]) + score);
                    }
                } else {
                    Collections.addAll(filterSet, StringUtils.split(value2string, COMMA));
                }
            }

            StringBuilder sb = getResult(filterMap(temp_map, filterSet));
            if (sb.length() > 0) {
                outputValue.set(sb.toString());
                context.write(key, outputValue);
            }
        }

        private StringBuilder getResult(HashMap<String, Float> map) {
            //按分數由大到小排序
            List<Map.Entry<String, Float>> list = new ArrayList<Map.Entry<String, Float>>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
                public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {
                    return (int) ((o2.getValue() - o1.getValue()) * 1000);
                }
            });
            map = null;
            StringBuilder sb = new StringBuilder();
            int min = Math.min(RECOMMENDATIONS_COUNT, list.size());
            list = list.subList(0, min);
            for (int index = 0; index < min; index++) {
                if (index > 0)
                    sb.append(COMMA);
                sb.append(list.get(index).getKey());
            }

            return sb;
        }

        private HashMap<String, Float> filterMap(HashMap<String, Float> map, HashSet<String> set) {
            Float value;
            for (String item : set) {
                value = map.get(item);
                if (value != null)  // 有取到東西，表示要移除
                    map.remove(item);
            }

            return map;
        }
    }
}
