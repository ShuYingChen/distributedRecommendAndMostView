package jobCore.ec1web.util;

import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Joan on 2014/9/5.
 */
public class Tool {
    public static final String ENCODING = "utf8";
    static final Pattern bot = Pattern.compile("bingbot|Googlebot|Yahoo! Slurp|Baiduspider|msnbot|iaskspider|sogou|Price Detector|bitlybot|BingPreview|JikeSpider|Ezooms|org_bot|EasouSpider|EtaoSpider|ExaleadCloudview|facebookexternalhit|Feedfetcher-Google|AndroidDownloadManager|AppEngine-Google|PChomebot");
    static final Pattern request = Pattern.compile("\"GET[^\"]+\"");
    static final Pattern userAgent = Pattern.compile("\"[^\"]+\"");
    static final Pattern jsonFormat = Pattern.compile("record.htm?.*%22%5D%7D");

    public static boolean isBot(String log) {
        return bot.matcher(log).find();
    }

    public static String getRequest(String log) {
        Matcher m = request.matcher(log);
        return m.find() ? m.group() : "";
    }

    public static String getJsonData(String request) {
        String s = "";
        Matcher m = jsonFormat.matcher(request);
        if (m.find()) {
            s = m.group();
            s = s.replace("record.htm?", "");
        }
        return s;
    }

    public static String getIP(String log) {
        for (String s : StringUtils.split(log, " ")) {
            if (s.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}"))
                return s;
        }

        return "";
    }

    public static String getUserAgent(String log) {
        Matcher m = userAgent.matcher(log);
//        ArrayList<String> result = new ArrayList<String>(3);
        Pattern pattern = Pattern.compile("mozilla");
        Matcher matcher;
        String s = "";
        while (m.find()) {
            s = m.group();
            matcher = pattern.matcher(s);
            if (matcher.find()) {
                break;
            }
        }

        return s;
    }

    public static boolean isKnownDevice(String log, Pattern pattern) {
        return pattern.matcher(log).find();
    }

    public static List<Map.Entry<String, Integer>> sortMap(HashMap<String, Integer> map) {
        List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2) {
                return entry2.getValue() - entry1.getValue();
            }
        });
        return list;
    }
}
