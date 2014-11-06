package jobUtil;

/**
 * 測試區
 * Created by Joan on 2014/4/8.
 */
public class TestArea {
    static double startTime;
    static double endTime;
    static double totTime;
    static long before_memory;
    static long after_memory;
    static long used_memory;

    public static void main(String[] args) throws Exception {
        String s = "False";
        boolean b = Boolean.valueOf(s);
        System.out.println(b);
    }

    public static String convertHexToString(String hex) {
        String[] sArray = hex.replace("\\", "").split("x");
        String result = "";
        for (String x : sArray)
            result += x;
        hex = result;

        StringBuilder sb = new StringBuilder();
        StringBuilder temp = new StringBuilder();

        //49204c6f7665204a617661 split into two characters 49, 20, 4c...
        for (int i = 0; i < hex.length() - 1; i += 2) {
            //grab the hex in pairs
            String output = hex.substring(i, (i + 2));
            //convert hex to decimal
            int decimal = Integer.parseInt(output, 16);
            //convert the decimal to character
            sb.append((char) decimal);
            temp.append(decimal);
        }

        return sb.toString();
    }

    static void start() {
        Runtime.getRuntime().gc();
        startTime = System.currentTimeMillis();
        before_memory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    static void end(String name) {
        endTime = System.currentTimeMillis();
        totTime = endTime - startTime;
        after_memory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        used_memory = after_memory - before_memory;
        System.out.println(name + " : " + (used_memory / 1024) + " KB" + "\t" + "Using Time: " + totTime / 1000 + " sec");
        Runtime.getRuntime().gc();
    }
}
