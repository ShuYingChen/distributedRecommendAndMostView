package jobUtil;

import org.json.simple.JSONArray;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static jobCore.ec1web.util.Tool.*;
import static jobUtil.GlobalTool.*;

/**
 * 測試區
 * Created by Joan on 2014/9/5.
 */
public class TestArea {
    public static void main(String[] args) throws Exception {
        String s = "Sep  4 00:00:00 ec1web11 apache2: 180.218.205.154 - - [04/Sep/2014:00:00:00 +0800] \"GET /emon/v2/record.htm?%7B%22Uid%22%3A%226a1e2f13f280138235919ec1a40d097b5eb476de%22%2C%22Site%22%3A%2224h%22%2C%22Page%22%3A%22store%22%2C%22Action%22%3A%22pageview%22%2C%22Prod%22%3A%5B%22DGBO3J-A81426340-000%22%5D%7D&_=1409760000948&callback=jQuery17103325521079823375_1409759972518 HTTP/1.1\" 200 26 \"http://shopping.pchome.com.tw/?SR_NO=DGBO3J&func=style_show&mod=store&page=3\" \"Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53\" uid=a3506b05a9c6667f897892c70eb58a3d ecc=95142a9f9c860ef7071ed75f38b3bdaf\n" +
                "Sep  4 00:00:01 ec1web11 apache2: 210.242.43.90 - - [04/Sep/2014:00:00:01 +0800] \"GET /emon/v2/record.htm?%7B%22Site%22%3A%22ecssl%22%2C%22Page%22%3A%22cart%22%2C%22Uid%22%3A%22361a68365ec8dad694226fc90ace30a8948da993%22%2C%22Action%22%3A%22removecart%22%2C%22Prod%22%3A%5B%22DAAG4S-A9005A8SP-000%22%5D%7D HTTP/1.1\" 200 - \"https://ecssl.pchome.com.tw/sys/cflow/?0xb9974fa3cff40124ee53e219a01f7f04e1bfe89aba504d535ea173d9d9564206171c9c43ebc2be7a8ff7e34960fac0bad84c2cd7955d0456\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko\" uid=- ecc=-\n" +
                "Sep  4 00:00:03 ec1web11 apache2: 175.182.119.226 - - [04/Sep/2014:00:00:03 +0800] \"GET /emon/v2/record.htm?%7B%22Uid%22%3A%228746455bcff9e2b6fc14bae2b7050cd5875e91cb%22%2C%22Site%22%3A%2224h%22%2C%22Page%22%3A%22search%22%2C%22Action%22%3A%22addcart%22%2C%22Prod%22%3A%5B%22DCAY12-A79760001-000%22%5D%7D&_=1409760025039&callback=jQuery17105646131944376975_1409758777351 HTTP/1.1\" 200 26 \"http://ecshweb.pchome.com.tw/search/v3.2/?q=SONY%20SBH52\" \"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36\" uid=72424c34db2c43cae6c350a50adce518 ecc=3360a9986d1c3988fe63e3e9be7e1ae1\n" +
                "Sep  4 00:00:04 ec1web11 apache2: 124.218.76.55 - - [04/Sep/2014:00:00:04 +0800] \"GET /emon/v2/record.htm?%7B%22Uid%22%3A%22b2e6191ca9cab40c7cc67bbdbca4956c7f1c40fe%22%2C%22Site%22%3A%22ecshop%22%2C%22Page%22%3A%22store%22%2C%22Action%22%3A%22pageview%22%2C%22Prod%22%3A%5B%22CCAA5W-A71960979-000%22%5D%7D&_=1409760120836&callback=jQuery17106118161171436546_1409760115613 HTTP/1.1\" 200 26 \"http://shopping.pchome.com.tw/?SR_NO=CCAA07&func=style_show&mod=store&page=3\" \"Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0; Touch; MASPJS)\" uid=9dfa013ea45046e62e3d084e5d9f1918 ecc=0e2c2e0edf315965725f805c4a89add4\n" +
                "Sep  4 00:00:04 ec1web11 apache2: 58.115.25.183 - - [04/Sep/2014:00:00:04 +0800] \"GET /emon/v2/record.htm?%7B%22Uid%22%3A%2254c224828bf01468b2fd240388db6758f1d67dae%22%2C%22Site%22%3A%2224h%22%2C%22Page%22%3A%22prod%22%2C%22Action%22%3A%22addcart%22%2C%22Prod%22%3A%5B%22DBAK0B-181861145-000%22%5D%7D HTTP/1.1\" 200 26 \"http://24h.pchome.com.tw/prod/DBAK0B-181861145\" \"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)\" uid=e8d0a12c625fc064b4dfb050f317f115 ecc=d782fccdbf04ff278e60b26c8f75c45e\n" +
                "Sep  4 00:00:05 ec1web11 apache2: 122.117.188.109 - - [04/Sep/2014:00:00:05 +0800] \"GET /emon/v2/record.htm?%7B%22Uid%22%3A%220c0ad73c7f80da060912e0fbd474092428456560%22%2C%22Site%22%3A%22ecshop%22%2C%22Page%22%3A%22store%22%2C%22Action%22%3A%22pageview%22%2C%22Prod%22%3A%5B%22AEAB5L-A74589207-000%22%5D%7D&_=1409760045292&callback=jQuery17104508145914878696_1409760042103 HTTP/1.1\" 200 26 \"http://shopping.pchome.com.tw/AEAB5L\" \"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.172 Safari/537.22\" uid=689c2e8658bfc5576f20cd926e9ee55c ecc=4d9dec0088871ff77bf1c7663235a32a\n" +
                "Sep  4 00:00:07 ec1web11 apache2: 49.158.0.7 - - [04/Sep/2014:00:00:07 +0800] \"GET /emon/v2/record.htm?%7B%22Uid%22%3A%224a80eef4fcdbad9e465de9d22a2d892a4fdbf3d7%22%2C%22Site%22%3A%22ecshop%22%2C%22Page%22%3A%22store%22%2C%22Action%22%3A%22pageview%22%2C%22Prod%22%3A%5B%22DRAF1U-A61529533-000%22%5D%7D&_=1409760002941&callback=jQuery17108285344541072845_1409760000351 HTTP/1.1\" 200 26 \"http://shopping.pchome.com.tw/?SR_NO=AGAC4I&func=style_show&mod=store&page=1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36\" uid=f553a6066a895bacdbabb2cdf11a45e6 ecc=e88beb616a0bd3a8118cbc729c43bc6c\n" +
                "Sep  4 00:00:07 ec1web11 apache2: 115.43.218.217 - - [04/Sep/2014:00:00:07 +0800] \"GET /emon/v2/record.htm?%7B%22Uid%22%3A%225538188973a08db0b781cfd6e1204c066aa36975%22%2C%22Site%22%3A%22ecshop%22%2C%22Page%22%3A%22store%22%2C%22Action%22%3A%22pageview%22%2C%22Prod%22%3A%5B%22ACAL10-A43982987-000%22%5D%7D&_=1409760036388&callback=jQuery17103671767346095294_1409760018923 HTTP/1.1\" 200 26 \"http://shopping.pchome.com.tw/?mod=store&func=style_show&SR_NO=ACAC5F\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36\" uid=861ed97c3a8be80f09d17310474e678c ecc=c5766a9d9fb74408cb211c344e9ba38c\n" +
                "Sep  4 00:00:07 ec1web11 apache2: 123.193.69.188 - - [04/Sep/2014:00:00:07 +0800] \"GET /emon/v2/record.htm?%7B%22Uid%22%3A%22fa26a59ce7a5d604aa8a11b90fe62f17ee503c50%22%2C%22Site%22%3A%2224h%22%2C%22Page%22%3A%22store%22%2C%22Action%22%3A%22pageview%22%2C%22Prod%22%3A%5B%22DHAFAT-19005BRN6-000%22%5D%7D&_=1409760006308&callback=jQuery171001720240293070674_1409759933508 HTTP/1.1\" 200 26 \"http://24h.pchome.com.tw/?mod=store&func=style_show&SR_NO=DHAF8W\" \"Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) GSA/4.1.0.31802 Mobile/11D257 Safari/9537.53\" uid=265445b03864f265c179a2218609bc4d ecc=dc06a2923d237cdbfaa66998d5fd7f08\n" +
                "Sep  4 00:00:08 ec1web11 apache2: 210.242.43.90 - - [04/Sep/2014:00:00:08 +0800] \"GET /emon/v2/record.htm?%7B%22Site%22%3A%22ecssl%22%2C%22Page%22%3A%22cart%22%2C%22Uid%22%3A%22fe90803ad2e8d09184d317df5f07ff8be5cf2d47%22%2C%22Action%22%3A%22removecart%22%2C%22Prod%22%3A%5B%22DEAKDW-A81886968-002%22%5D%7D HTTP/1.1\" 200 - \"https://ecssl.pchome.com.tw/sys/cflow/?0xb9974fa3cff40124ee53e219a01f7f04e1bfe89aba504d535ea173d9d9564206171c9c43ebc2be7a8ff7e34960fac0bad84c2cd7955d0456\" \"Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; MAAU)\" uid=- ecc=-";
//        for (String text : StringUtils.split(s, "\n")) {
////        String log = "Sep  4 00:00:00 ec1web11 apache2: 180.218.205.154 - - [04/Sep/2014:00:00:00 +0800] \"GET /emon/v2/record.htm?%7B%22Uid%22%3A%226a1e2f13f280138235919ec1a40d097b5eb476de%22%2C%22Site%22%3A%2224h%22%2C%22Page%22%3A%22store%22%2C%22Action%22%3A%22pageview%22%2C%22Prod%22%3A%5B%22DGBO3J-A81426340-000%22%5D%7D&_=1409760000948&callback=jQuery17103325521079823375_1409759972518 HTTP/1.1\" 200 26 \"http://shopping.pchome.com.tw/?SR_NO=DGBO3J&func=style_show&mod=store&page=3\" \"Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53\" uid=a3506b05a9c6667f897892c70eb58a3d ecc=95142a9f9c860ef7071ed75f38b3bdaf";
//            String request = getRequest(text);
//            request = getJsonData(request);
//            request = URLDecoder.decode(request, ENCODING);
//            JSONObject json = (JSONObject) JSONValue.parse(request);
//            JSONArray jsonArray = (JSONArray) json.get(JsonKey.Prod.name());
//            System.out.println("*******");
//        }
//        String path = "C:\\Users\\Joan\\Downloads\\-temp_joan-analy_eclweb_log-result-part-m-00000";
        String ori_path = "D:\\code_space\\analysisAccessLog\\data\\PageViewHits\\eclweb11_log.txt";
        readFile(ori_path);


    }

    public static void readFile(String filePath) throws IOException {
        // "http://shopping.pchome.com.tw/DPAD8O"
        // "http://shopping.pchome.com.tw/?mod=store&func=style_show&SR_NO=DGBJA7"
        // "http://24h.pchome.com.tw/?mod=store&func=style_show&SR_NO=DCBA7H"
        Pattern store_pattern = Pattern.compile("SR_NO=\\w{6}|\\.tw/\\w{6}\"");

        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line, request, site, page, action, s = "";
//        String match = "Page%22%3A%22store%22%2C%22Action%22%3A%22pageview";
        String match = "\"Page\":\"store\",\"Action\":\"pageview\"";
//        String match_order = "\"Action\":\"order\"";
        JSONArray prod;
        Pattern p = Pattern.compile(match);
        Matcher m;
        while (br.ready()) {
            line = br.readLine();
//            System.out.println(line);
            request = getRequest(line);  //點擊的資料模型
            request = getJsonData(request); //只取 json 資料的部分
            if (!request.isEmpty()) {
                request = URLDecoder.decode(request, ENCODING); //解碼
                request = request.replaceAll("\"\"", "\""); //有些會有雙引號
            }
            m = p.matcher(request);
            if (m.find()) {
                m = store_pattern.matcher(line);
//                System.out.print(request + TAB);
                if (m.find()) {
                    s = m.group();
                    s = s.contains("SR_NO") ? s.replace("SR_NO=", "") : s.replace(".tw/", "").replace("\"", "");
//                    System.out.println(s);
                } else {
                    System.out.println(URLDecoder.decode(line, ENCODING));
                }
            }
//            request = getRequest(line);  //點擊的資料模型
//            request = getJsonData(request); //只取 json 資料的部分
//            if (!request.isEmpty()) {
//                request = URLDecoder.decode(request, ENCODING); //解碼
//                request = request.replaceAll("\"\"", "\""); //有些會有雙引號
//                if (p.matcher(request).find()) {
//                    JSONObject json = null;
//                    try {
//                        json = (JSONObject) JSONValue.parse(request);
//                    } catch (NullPointerException e) {
//                        System.out.println(request);
//                        System.out.println(Arrays.toString(e.getStackTrace()));
//                    }
//
//                    if (json != null) {
//                        HashSet<String> item_set = new HashSet<String>();
//                        site = (String) json.get(JsonKey.Site.name());
//                        page = (String) json.get(JsonKey.Page.name());
//                        action = (String) json.get(JsonKey.Action.name());
//                        prod = (JSONArray) json.get(JsonKey.Prod.name());
//                        for (Object s : prod.toArray())
//                            item_set.add(((String) s));
//                        for (String item : item_set) {
//                            item = item.substring(0, item.lastIndexOf("-"));
//                            System.out.println(item);
//                        }
//                    }
//                }
//            }
        }
        br.close();
    }

    public static List<String> list = new ArrayList<String>();

    public static HashMap<Integer, ArrayList<String>> testMonth() throws java.text.ParseException {
        Calendar c1 = Calendar.getInstance();
        c1.setTime(dateStringFormat.parse(currentMonthStartDate));
        ArrayList<String> dayOfWeek;
        HashMap<Integer, ArrayList<String>> weekOfMonth = new HashMap<Integer, ArrayList<String>>(7);
        int date = c1.get(Calendar.DATE);
        // 1代表星期日、2代表星期1、3代表星期二
        int day = c1.get(Calendar.DAY_OF_WEEK);
        day = (day + 4) % 7;    //轉換一週的第一天, 星期三為第一天
        String currentDay = currentMonthStartDate;
        int thisWeek = 1;   //第一周
        while (!currentDay.equals(currentMonthEndDate)) {
            currentDay = dateStringFormat.format(c1.getTime());
            dayOfWeek = weekOfMonth.get(thisWeek);
            if (dayOfWeek == null) dayOfWeek = new ArrayList<String>(7);

            if (date == 1 && day != 1) {  //本月的一開始 && 不是一周的第一天
                Calendar c2 = Calendar.getInstance();
                c2.setTime(dateStringFormat.parse(currentMonthStartDate));
                c2.add(Calendar.DATE, -1);  //減一天
                String last_month_date = dateStringFormat.format(c2.getTime());
                int last_month_day = c2.get(Calendar.DAY_OF_WEEK);
                last_month_day = (last_month_day + 4) % 7;
                while (last_month_day != 0) {
                    dayOfWeek.add(last_month_date);
                    c2.add(Calendar.DATE, -1);  //減一天
                    last_month_date = dateStringFormat.format(c2.getTime());
                    last_month_day = c2.get(Calendar.DAY_OF_WEEK);
                    last_month_day = (last_month_day + 4) % 7;
                }
            }

            dayOfWeek.add(currentDay);
            weekOfMonth.put(thisWeek, dayOfWeek);
            if (day == 0) { //星期三是第一天
                thisWeek++;
            }
            c1.add(Calendar.DATE, 1);
            date = c1.get(Calendar.DATE);
            day = c1.get(Calendar.DAY_OF_WEEK);
            day = (day + 4) % 7;
        }
        return weekOfMonth;
    }

}