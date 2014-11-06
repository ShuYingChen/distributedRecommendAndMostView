package hadoopJobCore.recommend.region;

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

import static jobUtil.Tool.*;

/**
 * 分散式區推薦
 * 計算共現次數
 * Created by Joan on 2014/9/3.
 */
public class RegionItemVector {
    // RegionItemVector 的結果輸出路徑
    public final static String REGION_ITEM_VECTOR_OUTPUT = "/tmp/disRegionRecommend/RegionItemVector";

    /**
     * input → DEBG_DEBG2H-A70201693,DEBG_DEBG2H-A57761725
     * output → DEBG_DEBG2H-A70201693	AHAE_AHAE5K-A70973596:2,AHAE_AHAE5K-A61808768:3,AHAE_AHAE4D-A70354986:1
     */
    public static class RegionItemVectorMapper extends Mapper<LongWritable, Text, Text, Text> {
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

    public static class RegionItemVectorCombiner extends Reducer<Text, Text, Text, Text> {
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
     * input → DEBG_DEBG2H-A70201693	AHAE_AHAE5K-A70973596:2,AHAE_AHAE5K-A61808768:3,AHAE_AHAE4D-A70354986:1
     * output → DEBG_DEBG2H-A70201693 AHAE-A70973596:2,AHAE-A61808768:3,AHAE-A70354986:1
     */
    public static class RegionItemVectorReducer extends Reducer<Text, Text, Text, Text> {
        public static final String TOP_COUNT_PER_REGION_ITEM = "topCountPerRegionItem";
        public static int TOP = 50;   // 先設每筆取 top 50
        Text output_value = new Text();

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            TOP = conf.getInt(TOP_COUNT_PER_REGION_ITEM, TOP);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TObjectIntHashMap<String> pair = new TObjectIntHashMap<String>();
            String key_region = key.toString().substring(0, 4);
            int count;
            String[] tempArray, region_item;
            for (Text value : values) {
                for (String itemCount : StringUtils.split(value.toString(), COMMA)) {
                    tempArray = StringUtils.split(itemCount, COLON);
                    count = Integer.parseInt(tempArray[1]);
                    region_item = StringUtils.split(tempArray[0], UNDERLINE);
                    if (!key_region.equals(region_item[0])) //區編不相同才納入結果集
                        pair.adjustOrPutValue(region_item[1], count, count); //只取品編來加總
                }
            }

            Vector<RecommendItem> recommendItemVector = getItemVector(key_region, pair);    //取得排序過後的推薦結果
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

        private Vector<RecommendItem> getItemVector(String region, TObjectIntHashMap<String> pair) {
            region = region.substring(0, 3);
            int score;
            String item_region;
            Vector<RecommendItem> v = new Vector<RecommendItem>(pair.size());
            for (String item : pair.keySet()) {
                item_region = item.substring(0, 3);
                score = pair.get(item);
                if (item_region.equals(region))
                    score = score << 1; // 左移1位等同於乘以 2

                v.add(new RecommendItem(item, score));
            }

            Collections.sort(v, new RecommendItemComparator());
            int min = Math.min(TOP, v.size());
            v.setSize(min);
            v.trimToSize();
            pair.clear();
            return v;
        }
    }
}
