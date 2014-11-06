package jobUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Joan on 2014/7/4.
 */
public class Tool {
    public static Pattern bot = Pattern.compile("bingbot|Googlebot|Yahoo! Slurp|Baiduspider|msnbot|iaskspider|sogou|Price Detector|bitlybot|BingPreview|JikeSpider|Ezooms|org_bot|EasouSpider|EtaoSpider|ExaleadCloudview|facebookexternalhit|Feedfetcher-Google|AndroidDownloadManager|AppEngine-Google|Sosospider|122.147.50.[\\d]{1,3}|54.225.101.193, |54.235.93.182, |PChomebot|ecsearchbot|ecseobot");
    public static final String UNDERLINE = "_";
    public static final String TAB = "\t";
    public static final String COLON = ":";
    public static final String COMMA = ",";
    public static final String STRAIGHT = "|";
    public static final String VIEW = "view";
    public static final String CART = "cart";
    public static final String BUY = "buy";
    public static final String EID = "eid";
    public static final String UID = "uid";
    public static final String noTime = "noTime";
    public static final String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
    public static DecimalFormat df = new DecimalFormat("#.###");
    public static final String MODE = "MODE";

    /**
     * 如果不用一個 thread 來執行，會導致 job 執行完之後不會退出，但結果是正常的。
     * 所以不要直接用 jobControl.run();
     * 否則要檢查 java 程序是否有正常關閉
     *
     * @param job1 first job
     * @param job2 second job
     * @param name JobControl name
     * @return 0 or 1
     * @throws InterruptedException
     */
    public static int controlJobProcess(ControlledJob job1, ControlledJob job2, String name) throws InterruptedException {
        JobControl jobControl = new JobControl(name);
        jobControl.addJob(job1);
        jobControl.addJob(job2);
        job2.addDependingJob(job1);   //job1 做完才呼叫 job2 執行
        Thread theController = new Thread(jobControl);
        theController.start();
        while (true) {
            if (jobControl.allFinished()) {
                System.out.println(jobControl.getSuccessfulJobList());
                jobControl.stop();
                return 0;
            }

            if (jobControl.getFailedJobList().size() > 0) {
                System.out.println(jobControl.getFailedJobList());
                jobControl.stop();
                return 1;
            }
        }
    }

    /**
     * 取得昨天的日期
     * 格式為 yyyyMMdd
     *
     * @return 昨天
     */
    public static String getYesterday() {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);    //減一天
        return new SimpleDateFormat("yyyyMMdd").format(calendar.getTime());
    }

    public static int inputDates(String inputData) throws IOException {
        HashSet<String> set = new HashSet<String>();
        Pattern p = Pattern.compile("\\d{4}/\\d{2}");
        Matcher m;
        for (String input : StringUtils.split(inputData, COMMA)) {
            m = p.matcher(input);
            if (m.find())
                set.add(m.group());
        }
        return (set.size() > 0) ? set.size() : 1;
    }

    public static HashSet<String> getItemSet(Iterable<Text> values) {
        HashSet<String> temp = new HashSet<String>();
        String item;
        for (Text itemNO : values) {
            item = itemNO.toString();
            if (item.contains(COMMA)) {
                Collections.addAll(temp, StringUtils.split(item, COMMA));
            } else {
                temp.add(item);
            }
        }
        return temp;
    }

    public static StringBuffer getItemsVector(HashSet<String> set) {
        StringBuffer buffer = new StringBuffer();
        int index = 0;
        for (String item : set) {
            if (index > 0)
                buffer.append(COMMA);
            buffer.append(item);
            index++;
        }

        return buffer;
    }

    public static List<Map.Entry<String, Integer>> sortMap(HashMap<String, Integer> optionMap) {
        List<Map.Entry<String, Integer>> list_Data = new ArrayList<Map.Entry<String, Integer>>(optionMap.entrySet());
        Collections.sort(list_Data, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2) {
                if (entry1.getValue() > entry2.getValue())
                    return -1;
                if (entry1.getValue() < entry2.getValue())
                    return 1;
                return 0;
            }
        });
        return list_Data;
    }
}
