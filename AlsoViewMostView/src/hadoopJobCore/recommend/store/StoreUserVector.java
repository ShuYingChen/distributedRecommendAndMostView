package hadoopJobCore.recommend.store;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static jobUtil.Tool.getItemSet;
import static jobUtil.Tool.getItemsVector;

/**
 * 分散式館推薦
 * 先整理 user 的瀏覽商品
 * Created by Joan on 2014/8/11.
 */
public class StoreUserVector {
    // StoreUserVector 的結果輸出路徑
    public final static String STORE_USER_VECTOR_OUTPUT = "/tmp/disStoreRecommend/StoreUserVector";

    /**
     * input → 00c9ade79e49dc03ef57af6c13f851eca83ebb3f	DEBG2H-A70201693 DEBG2H  DEBG    5
     * output → 00c9ade79e49dc03ef57af6c13f851eca83ebb3f    DEBG2H_DEBG2H-A70201693
     */
    public static class StoreUserVectorMapper extends Mapper<Text, Text, Text, Text> {
        final String user_format = "\\w{40}";
        final String item_format = "\\w{6}_\\w{6}-\\w{9}";

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String user_string, item_string;
            user_string = key.toString();
            item_string = value.toString();
            if (user_string.matches(user_format) && item_string.matches(item_format)) {
                context.write(key, value);
            }
        }
    }

    /**
     * output → 00c9ade79e49dc03ef57af6c13f851eca83ebb3f    DEBG2H_DEBG2H-A70201693,DJAD3J_DJAD3J-A57761725...
     */
    public static class StoreUserVectorCombiner extends Reducer<Text, Text, Text, Text> {
        Text store_items = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = getItemsVector(getItemSet(values));
            if (sb.length() > 0) {
                store_items.set(sb.toString());
                context.write(key, store_items);
            }
        }
    }

    /**
     * output → DEBG2H_DEBG2H-A70201693,DJAD3J_DJAD3J-A57761725
     */
    public static class StoreUserVectorReducer extends Reducer<Text, Text, Text, NullWritable> {
        Text store_items = new Text();
        final NullWritable nullWritable = NullWritable.get();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = getItemsVector(getItemSet(values));
            if (sb.length() > 0) {
                store_items.set(sb.toString());
                context.write(store_items, nullWritable);
            }
        }
    }
}
