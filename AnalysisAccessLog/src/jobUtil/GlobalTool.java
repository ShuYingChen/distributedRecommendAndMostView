package jobUtil;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by Joan on 2014/9/17.
 */
public class GlobalTool {
    public static final String COLON = ":";
    public static final String UNDERLINE = "_";
    public static final String COMMA = ",";
    public static final String TAB = "\t";
    public static final SimpleDateFormat dateStringFormat = new SimpleDateFormat("yyyyMMdd");
    public static final SimpleDateFormat date2WeekFormat = new SimpleDateFormat("W");   //轉換當月第幾週
    public static String currentMonthStartDate = "";
    public static String currentMonthEndDate = "";
    public static HashMap<Integer, ArrayList<String>> weekOfMonth = new HashMap<Integer, ArrayList<String>>(7);
    public static String exeDate = "";
    public static int thisWeek = 0;

    public static void setExeDate(String date) {
        exeDate = date;
    }

    public static void setThisWeek(int week) {
        thisWeek = week;
    }

    public static String getJobDate(String output) {
        int index = output.lastIndexOf(File.separator);
        return output.substring(index + 1);
    }

    public static String getToday() {
        return dateStringFormat.format(new Date());
    }

    public static String getYesterday() {
        Date date = new Date();
        Calendar c = new GregorianCalendar();
        c.setTime(date);
        c.add(Calendar.DATE, -1);
        date = c.getTime();
        return dateStringFormat.format(date);
    }

    /**
     * 星期日為一週的開始
     *
     * @param dateString 日期
     * @return 第幾週
     */
    public static String getThisWeek(String dateString) {
        String thisWeek;
        try {
            Date date = dateStringFormat.parse(dateString);
            thisWeek = date2WeekFormat.format(date);
        } catch (ParseException e) {
            System.out.println("fail parse date [" + dateString + "]");
            thisWeek = "1"; //預設值為第一週
            e.printStackTrace();
        }
        return thisWeek;
    }

    /**
     * 取得本月第一天，即20141101
     *
     * @return
     */
    public static void setCurrentMonthStartTime() {
        Calendar c = Calendar.getInstance();
        try {
            c.setTime(dateStringFormat.parse(exeDate));
            c.set(Calendar.DATE, 1);
            currentMonthStartDate = dateStringFormat.format(c.getTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 取得本月最後一天，即20141130
     *
     * @return
     */
    public static void setCurrentMonthEndTime() {
        Calendar c = Calendar.getInstance();
        try {
            c.setTime(dateStringFormat.parse(exeDate));
            c.set(Calendar.DATE, 1);
            c.add(Calendar.MONTH, 1);
            c.add(Calendar.DATE, -1);
            currentMonthEndDate = dateStringFormat.format(c.getTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void prepareThisMonthData(String executeDate) {
        setExeDate(executeDate);    //準備一個月份中每週的資料
        setCurrentMonthStartTime();
        setCurrentMonthEndTime();
        Calendar c1 = Calendar.getInstance(), c2 = Calendar.getInstance();
        try {
            c1.setTime(dateStringFormat.parse(currentMonthStartDate));
            c2.setTime(dateStringFormat.parse(currentMonthStartDate));
        } catch (Exception e) {
            e.printStackTrace();
        }
        ArrayList<String> dayOfWeek;
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
            if (currentDay.equals(exeDate)) {
                setThisWeek(thisWeek);
            }
            if (day == 0) { //星期三是第一天
                thisWeek++;
            }
            c1.add(Calendar.DATE, 1);
            date = c1.get(Calendar.DATE);
            day = c1.get(Calendar.DAY_OF_WEEK);
            day = (day + 4) % 7;
        }
    }

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
    public static int controlJobProcess(ControlledJob job1, ControlledJob job2, ControlledJob job3, String name) throws InterruptedException {
        JobControl jobControl = new JobControl(name);
        jobControl.addJob(job1);
        jobControl.addJob(job2);
        jobControl.addJob(job3);
        job2.addDependingJob(job1);   //job1 做完才呼叫 job2 執行
        job3.addDependingJob(job1);   //job1 做完才呼叫 job3 執行
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

    public static int controlJobProcess(String name, ControlledJob... jobs) throws InterruptedException {
        JobControl jobControl = new JobControl(name);
        ArrayList<ControlledJob> list = new ArrayList<ControlledJob>();
        Collections.addAll(list, jobs);
        for (int i = 0; i < list.size(); i++) {
            jobControl.addJob(list.get(i));
            if (i > 0) {
                list.get(i).addDependingJob(list.get(i - 1));
            }
        }
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
}
