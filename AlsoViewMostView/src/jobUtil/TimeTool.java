package jobUtil;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 時間計算的相關工具
 * Created by Joan on 2014/3/28.
 */
public class TimeTool {
    private static DateFormat df = new SimpleDateFormat("yyyyMMdd");

    /**
     * 計算兩個日期相差天數
     * @param start_time 起始時間
     * @param end_time 結束時間
     * @return 天數
     */
    public static long numDateBetween(String start_time, String end_time) {
        int sec_per_day = 24 * 60 * 60 * 1000;
        long day = 0;
        //日期相减算出秒的算法
        Date date1, date2;
        try {
            date1 = df.parse(start_time);
            date2 = df.parse(end_time);
            //日期相减得到相差的日期
            day = (date1.getTime() - date2.getTime()) / sec_per_day > 0 ? (date1.getTime() - date2.getTime()) / sec_per_day :
                    (date2.getTime() - date1.getTime()) / sec_per_day;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return day;
    }

    /**
     * 比較兩個時間的前後
     * @param DATE1 第一個時間
     * @param DATE2 第二個時間
     * @return 1→前面小; 2→前面大
     */
    public static int compare_date(String DATE1, String DATE2) {
        try {
            Date dt1 = df.parse(DATE1);
            Date dt2 = df.parse(DATE2);
            if (dt1.getTime() <= dt2.getTime()) {
                //System.out.println(DATE1 + " 比 " + DATE2 + " 早");
                return 1;   //回傳 1 表示前面的日期比較小
            } else if (dt1.getTime() > dt2.getTime()) {
                //System.out.println(DATE1 + " 比 " + DATE2 + " 晚");
                return -1;  //回傳 -1 表示前面的日期比較大
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return 0;
    }
}
