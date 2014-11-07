package jobCore.ec1web.itemPageViewHits;

import jobCore.ec1web.util.Record;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

import static jobUtil.GlobalTool.TAB;

/**
 * 只取 page view 的部分
 * 並且歸下列 6 類
 * 1. 24h store
 * 2. ecshop store
 * 3. books (書店 site 算一起)
 * 4. mobile (手機 site 算一起)
 * 5. activity (活動頁算一起)
 * 6. store (剩餘的館頁一起)
 * Created by Joan on 2014/9/17.
 */
public class CatePVStatistic {
    public static class CatePVStatisticMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Text output_key = new Text();
        int line_count = 0;
        NullWritable nullWritable = NullWritable.get();
        HashMap<String, Record> mapping = new HashMap<String, Record>();
        HashMap<String, String> site_store_map = new HashMap<String, String>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String site, page, action, mapping_key;
            int pc, phone, tablet, other, unknown;
            String[] arr;
            Record record;
            if (line_count > 0) {
                arr = StringUtils.split(data, TAB);
                site = arr[0];
                page = arr[1];
                action = arr[2];
                pc = Integer.parseInt(arr[5]);
                phone = Integer.parseInt(arr[6]);
                tablet = Integer.parseInt(arr[7]);
                other = Integer.parseInt(arr[8]);
                unknown = Integer.parseInt(arr[9]);
                mapping_key = getMappingKey(site, page);    //取得對應的 key

                if (action.equals("pageview")) {
                    record = mapping.get(mapping_key);
                    if (record == null)
                        record = new Record();

                    if ((site.equals("24h") && page.equals("store")) || (site.equals("ecshop") && page.equals("store"))) {
                        data = data.replace("pageview" + TAB, "");
                        site_store_map.put(site, data);
                    } else if (site.equals("books")) {
                        plusCount(record, pc, phone, tablet, other, unknown);
                    } else if (site.equals("mobile")) {
                        plusCount(record, pc, phone, tablet, other, unknown);
                    } else if (page.equals("activity")) {
                        plusCount(record, pc, phone, tablet, other, unknown);
                    } else if (page.equals("store")) {
                        plusCount(record, pc, phone, tablet, other, unknown);
                    } else { // 其他未歸類的 page view
                        plusCount(record, pc, phone, tablet, other, unknown);
                    }
                    mapping.put(mapping_key, record);
                }
            } else {
                data = data.replace("action" + TAB, "");
                output_key.set(data);
                context.write(output_key, nullWritable);
            }
            line_count++;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            String[] site_arr = {"24h", "ecshop"};
            for (String site : site_arr) {
                output_key.set(site_store_map.get(site));
                context.write(output_key, nullWritable);
            }

            String[] key_arr = {"books\tall_page", "mobile\tall_page", "residue_site\tstore", "residue_site\tactivity", "else_site\telse_page"};
            String info;
            Record record;
            for (String key : key_arr) {
                record = mapping.get(key);
                if (record != null) {
                    int pc = 0, mobile = 0;
                    if (key.contains("mobile"))
                        mobile = record.getTotal();
                    else
                        pc = record.getTotal();
                    info = key + TAB + pc + TAB + mobile + TAB + record.getPCcount() + TAB + record.getPhoneCount()
                            + TAB + record.getTabletCount() + TAB + record.getOtherCount() + TAB + record.getUnknownCount();
                    output_key.set(info);
                    context.write(output_key, nullWritable);
                }
            }
        }

        private void plusCount(Record record, int pc, int phone, int tablet, int other, int unknown) {
            record.plusPCcount(pc);
            record.plusPhoneCount(phone);
            record.plusTabletCount(tablet);
            record.plusOtherCount(other);
            record.plusUnknownCount(unknown);
        }

        private String getMappingKey(String site, String page) {
            String key;
            if (site.equals("books") || site.equals("mobile")) {
                key = site + TAB + "all_page";
            } else if (page.equals("activity") || page.equals("store")) {
                key = "residue_site" + TAB + page;
            } else {
                key = "else_site" + TAB + "else_page";
            }

            return key;
        }
    }
}
