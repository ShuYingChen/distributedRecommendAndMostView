package jobUtil;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自製 Writable
 * 品編 : 次數
 * Created by Joan on 2014/6/11.
 */
public class ItemCount implements WritableComparable<jobUtil.ItemCount> {
    private Text item;
    private FloatWritable score;

    public ItemCount() {
        set(new Text(), new FloatWritable());
    }

    public ItemCount(String item, int score){
        set(new Text(item), new FloatWritable(score));
    }

    public ItemCount(Text item, FloatWritable score){
        set(item, score);
    }

    public void set(Text item, FloatWritable score) {
        this.item = item;
        this.score = score;
    }

    public String getItem(){
        return item.toString();
    }

    public float getCount(){
        return score.get();
    }

    public void write(DataOutput out) throws IOException {
        item.write(out);
        score.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        item.readFields(in);
        score.readFields(in);
    }

    public int hashCode() {
        return item.hashCode() * 163 + score.hashCode();
    }

    /**
     * 比品編就好
     *
     * @param o
     * @return 品編是否相同
     */
    public boolean equals(Object o) {
        if (o instanceof jobUtil.ItemCount) {
            jobUtil.ItemCount ic = (jobUtil.ItemCount) o;
            return item.toString().equals(ic.item.toString());
        }
        return false;
    }

    public String toString() {
        return item + ":" + score.get();
    }

    public int compareTo(jobUtil.ItemCount ic) {
        int cmp = item.compareTo(ic.item);
        if (cmp != 0) {
            return cmp;
        }
        return score.compareTo(ic.score);
    }
}
