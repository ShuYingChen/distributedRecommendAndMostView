package jobCore.ec1web.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 取得 uid 和 eid
 * uid → session (cookie)
 * eid → email
 * log 範例 :
 * Oct  7 16:46:57 ec1web24 apache2: 210.242.43.90 - - [07/Oct/2014:16:46:57 +0800] "GET /emon/uidmap.htm?uid=42ad5da3767d99bd59cd34c147fb001e738a96d8,eid=6f74f330a89c0fd7376283aa15236a863fe312a2,v= HTTP/1.1" 200 - "/sys/cflow/?0xa2b89572592d66d8bc9c1e36fe6c04bc2b8511856517213cb24542b9f850541964db688f6ea95621c230b1aeb441d233a1943a6a8af1687806356d1a2bb3b8979d7c42be4ea27a02d70d60477068954cac585cb4f17e0411b16c0218d49138553d405cc47a96cac9" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET4.0C; .NET4.0E)" uid=- ecc=-
 * Created by Joan on 2014/10/9.
 */
public class Uid2Eid {
    // uidmap 的 pattern
    public Pattern record_pattern = Pattern.compile("\"GET /emon/uidmap.htm[^\"]+\"");
    Pattern uid_eid_pattern = Pattern.compile("uid=[^,]+,|eid=[^,]+,");
    Matcher record_matcher, uid_eid_matcher;
    String UID; // uid 是存一年的，所以有可能會對應多個 eid (若同一台電腦有多個使用者 or 登入不同 email)
    String EID;

    public static void main(String[] args) {
        String log = "Oct  7 16:46:57 ec1web24 apache2: 210.242.43.90 - - [07/Oct/2014:16:46:57 +0800] \"GET /emon/uidmap.htm?uid=42ad5da3767d99bd59cd34c147fb001e738a96d8,eid=6f74f330a89c0fd7376283aa15236a863fe312a2,v= HTTP/1.1\" 200 - \"/sys/cflow/?0xa2b89572592d66d8bc9c1e36fe6c04bc2b8511856517213cb24542b9f850541964db688f6ea95621c230b1aeb441d233a1943a6a8af1687806356d1a2bb3b8979d7c42be4ea27a02d70d60477068954cac585cb4f17e0411b16c0218d49138553d405cc47a96cac9\" \"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET4.0C; .NET4.0E)\" uid=- ecc=-";
        Uid2Eid uid2Eid = new Uid2Eid();
        uid2Eid.parseUidAndEid(log);
        System.out.println(uid2Eid.getUID());
        System.out.println(uid2Eid.getEID());
    }

    /**
     * record_matcher → "GET /emon/uidmap.htm?uid=42ad5da3767d99bd59cd34c147fb001e738a96d8,eid=6f74f330a89c0fd7376283aa15236a863fe312a2,v= HTTP/1.1"
     * @param log access log
     */
    public void parseUidAndEid(String log) {
        String request, match;
        record_matcher = record_pattern.matcher(log);
        if (record_matcher.find()) {
            request = record_matcher.group();
            uid_eid_matcher = uid_eid_pattern.matcher(request);
            while (uid_eid_matcher.find()){
                match = uid_eid_matcher.group();
                if(match.contains("uid")){
                    UID = match.replaceAll("uid=|,", "");
                } else if(match.contains("eid")){
                    EID = match.replaceAll("eid=|,","");
                }
            }
        }
    }

    public String getUID() {
        return UID;
    }

    public String getEID() {
        return EID;
    }
}
