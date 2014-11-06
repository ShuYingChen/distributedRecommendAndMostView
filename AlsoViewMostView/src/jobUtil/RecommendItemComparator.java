package jobUtil;

import java.util.Comparator;

/**
 * RecommendItem 依照次數排序 - 遞減
 * Created by Joan on 2014/6/18.
 */
public class RecommendItemComparator implements Comparator {
    public int compare(Object o1, Object o2) {
        RecommendItem t1 = (RecommendItem) o1;
        RecommendItem t2 = (RecommendItem) o2;
        if (t1.score > t2.score) {
            return -1;
        }
        if (t1.score < t2.score) {
            return 1;
        }
        return 0;
    }
}
