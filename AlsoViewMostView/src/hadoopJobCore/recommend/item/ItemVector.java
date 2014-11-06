package hadoopJobCore.recommend.item;

import gnu.trove.map.hash.TObjectIntHashMap;
import jobUtil.RecommendItem;
import jobUtil.RecommendItemComparator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import static jobUtil.Tool.COLON;
import static jobUtil.Tool.COMMA;

/**
 * 計算共現次數
 * Created by Joan on 2014/6/12.
 */
public class ItemVector {

    /**
     * input → DEBG2H-A70201693,DJAD3J-A57761725
     * output → DEBG2H-A70201693	AHAE5K-A70973596:2,AHAE5K-A61808768:3,AHAE4D-A70354986:1
     */
    public static class ItemVectorMapper extends Mapper<LongWritable, Text, Text, Text> {
        public static final String ITEM_PER_MONTH = "itemPerMonth";
        public static int ITEM_NUMBER_PER_MONTH = 300;
        HashMap<String, TObjectIntHashMap<String>> outputData = new HashMap<String, TObjectIntHashMap<String>>();
        Text output_key = new Text(), output_value = new Text();
        public static final int OUTPUT_COUNTER = 1000;

        protected void setup(Context context) {
            int month = context.getConfiguration().getInt(ITEM_PER_MONTH, 1);
            if (month > 3)
                ITEM_NUMBER_PER_MONTH = month * 300;
            else
                ITEM_NUMBER_PER_MONTH = 900;
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            HashSet<String> itemSet = new HashSet<String>();
            Collections.addAll(itemSet, StringUtils.split(value.toString(), COMMA));
            if (itemSet.size() < ITEM_NUMBER_PER_MONTH) {
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
            outputData.clear(); // 寫完清空
            StringBuilder sb = new StringBuilder();
            TObjectIntHashMap<String> pair;
            int index;
            for (String firstItem : copy.keySet()) {
                pair = copy.get(firstItem);
                index = 0;
                for (String secondItem : pair.keySet()) {
                    if (index > 0)
                        sb.append(COMMA);
                    sb.append(secondItem).append(COLON).append(pair.get(secondItem));
                    index++;
                }
                if (sb.length() > 0) {
                    output_key.set(firstItem);
                    output_value.set(sb.toString());
                    context.write(output_key, output_value);
                }
                sb.setLength(0);
            }
        }
    }

    public static class ItemVectorCombiner extends Reducer<Text, Text, Text, Text> {
        Text output_value = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TObjectIntHashMap<String> trovePair = new TObjectIntHashMap<String>();
            int count;
            String[] tempArray;
            for (Text value : values) {
                for (String item_count : StringUtils.split(value.toString(), COMMA)) {
                    tempArray = StringUtils.split(item_count, COLON);
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

    /**
     * output → DEBG2H-A70201693	AHAE5K-A70973596,DEAV2D-A61808768,AHAE4D-A70354986
     */
    public static class ItemVectorReducer extends Reducer<Text, Text, Text, Text> {
        public static final String RECOMMENDATIONS_PER_ITEM = "recommendationsPerItem";
        public static int RECOMMENDATIONS_COUNT = 30;  // 輸出的推薦數量
        public static final String FILTER_COUNT = "filterCount";
        public static int filterCount = 0; // 過濾次數的商品，預設 0
        Text output_value = new Text();

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            RECOMMENDATIONS_COUNT = conf.getInt(RECOMMENDATIONS_PER_ITEM, RECOMMENDATIONS_COUNT);
            filterCount = conf.getInt(FILTER_COUNT, filterCount);
        }

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
                sb.append(recommendItem.item);
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
                    score = score << 1; // 左移1位等同於乘以 2

                if (score > filterCount) { //略過次數
                    v.add(new RecommendItem(item, score));
                }
            }

            Collections.sort(v, new RecommendItemComparator());
            v.setSize(Math.min(RECOMMENDATIONS_COUNT, v.size()));
            v.trimToSize();
            pair.clear();
            return v;
        }
    }
}