package jobUtil;

/**
 * 推薦商品的物件
 * Created by Joan on 2014/6/12.
 */
public class RecommendItem {
    public String item;
    public int score;
    public float recommend_score;

    public RecommendItem(String item, int score) {
        this.item = item;
        this.score = score;
    }

    public RecommendItem(String itemNo, float score){
        this.item = itemNo;
        this.recommend_score = score;
    }

    public void plusRecommendScore(float score){
        this.recommend_score = recommend_score + score;
    }

    public void setRecommendScore(int s) {
        this.score = s;
    }

    public String toString() {
        return "(recommend item: " + this.item + " | score: " + this.score + ")";
    }
}
