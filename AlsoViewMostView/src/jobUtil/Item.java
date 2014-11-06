package jobUtil;

/**
 * 商品資訊物件
 * Created by Joan on 2014/3/28.
 */
public class Item {
    public String item_no = "";
    public String startTime = "";
    public String endTime = "";
    public int viewCount = 0;
    public int cartCount = 0;
    public int buyCount = 0;

    public Item(String item){
        this.item_no = item;
    }

    public void setStartTime(String time){
        this.startTime = time;
    }

    public void setEndTime(String time){
        this.endTime = time;
    }

    public void setViewCount(int count){
        this.viewCount = count;
    }

    public void setCartCount(int count){
        this.cartCount = count;
    }

    public void setBuyCount(int count){
        this.buyCount = count;
    }
}
