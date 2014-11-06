package hadoopJobCore.recommend.store;

import gnu.trove.map.hash.TObjectIntHashMap;
import jobUtil.RecommendItem;
import jobUtil.RecommendItemComparator;
import jobUtil.Tool;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Vector;

import static jobUtil.Tool.COMMA;

/**
 * 統整館推薦結果
 * Created by Joan on 2014/8/12.
 */
public class StoreRecommend {
    /**
     * input → DEBG2H_DEBG2H-A70201693 AHAE5K-A70973596:2,AHAE5K-A61808768:3,AHAE4D-A70354986:1
     * output → DEBG2H  AHAE5K-A70973596:2,AHAE5K-A61808768:3,AHAE4D-A70354986:1
     */
    public static class StoreRecommendMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text output_key = new Text(), output_value = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String store, item_info;
            String[] value_array, store_item_array;
            value_array = StringUtils.split(value.toString(), Tool.TAB);
            store_item_array = StringUtils.split(value_array[0], Tool.UNDERLINE);
            store = store_item_array[0];
            item_info = value_array[1];
            if (store.length() > 0 && item_info.length() > 0) {
                output_key.set(store);
                output_value.set(item_info);
                context.write(output_key, output_value);
            }
        }
    }

    /**
     * output → DEBG2H  AHAE5K-A70973596:2,AHAE5K-A61808768:3,AHAE4D-A70354986:1
     */
    public static class StoreRecommendCombiner extends Reducer<Text, Text, Text, Text> {
        Text output_value = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TObjectIntHashMap<String> itemCountMap = new TObjectIntHashMap<String>();
            String item;
            int count, index = 0;
            String[] item_count_array;
            for (Text value : values) {
                for (String item_count : StringUtils.split(value.toString(), Tool.COMMA)) {
                    item_count_array = StringUtils.split(item_count, Tool.COLON);
                    item = item_count_array[0];
                    count = Integer.parseInt(item_count_array[1]);
                    itemCountMap.adjustOrPutValue(item, count, count);
                }
            }

            StringBuilder sb = new StringBuilder();
            for (String itemNo : itemCountMap.keySet()) {
                if (index > 0)
                    sb.append(Tool.COMMA);
                sb.append(itemNo).append(Tool.COLON).append(itemCountMap.get(itemNo));
                index++;
            }

            if (sb.length() > 0) {
                output_value.set(sb.toString());
                context.write(key, output_value);
            }
        }
    }

    public static class StoreRecommendReducer extends Reducer<Text, Text, Text, Text> {
        public static final String RECOMMENDATIONS_PER_ITEM = "recommendationsPerItem";
        public static int RECOMMENDATIONS_COUNT = 30;  // 輸出的推薦數量
        Text output_value = new Text();

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            RECOMMENDATIONS_COUNT = conf.getInt(RECOMMENDATIONS_PER_ITEM, RECOMMENDATIONS_COUNT);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TObjectIntHashMap<String> itemCountMap = new TObjectIntHashMap<String>();
            String item;
            int count;
            String[] item_count_array;
            for (Text value : values) {
                for (String item_count : StringUtils.split(value.toString(), Tool.COMMA)) {
                    item_count_array = StringUtils.split(item_count, Tool.COLON);
                    item = item_count_array[0];
                    count = Integer.parseInt(item_count_array[1]);
                    itemCountMap.adjustOrPutValue(item, count, count);
                }
            }

            Vector<RecommendItem> recommendStoreItemVector = getRecommendStoreItemVector(itemCountMap); // 取得排序結果
            RecommendItem recommendItem;
            StringBuilder sb = new StringBuilder();
            for (int index = 0; index < recommendStoreItemVector.size(); index++) {
                recommendItem = recommendStoreItemVector.get(index);
                if (index > 0)
                    sb.append(COMMA);
                sb.append(recommendItem.item);
            }

            if (sb.length() > 0) {
                output_value.set(sb.toString());
                context.write(key, output_value);
            }
        }

        private Vector<RecommendItem> getRecommendStoreItemVector(TObjectIntHashMap<String> pair) {
            Vector<RecommendItem> v = new Vector<RecommendItem>(pair.size());
            for (String item : pair.keySet()) {
                v.add(new RecommendItem(item, pair.get(item)));
            }

            Collections.sort(v, new RecommendItemComparator());
            v.setSize(Math.min(RECOMMENDATIONS_COUNT, v.size()));
            v.trimToSize();
            pair.clear();
            return v;
        }
    }
}
