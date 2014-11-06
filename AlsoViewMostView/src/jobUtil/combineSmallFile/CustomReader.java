package jobUtil.combineSmallFile;

import jobUtil.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * 測試將小的輸入檔合併
 * Created by Joan on 2014/7/24.
 */
public class CustomReader extends RecordReader<Text, Text> {
    private LineReader lr;
    private Text key = new Text();
    private Text value = new Text();
    private long start;
    private long end;
    private long currentPos;
    private Text line = new Text();
    private String mode = "item";   // default : 商品推薦

    public void initialize(InputSplit inputSplit, TaskAttemptContext cxt) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = cxt.getConfiguration();
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream is = fs.open(path);
        lr = new LineReader(is, conf);

        // 處理起始點和終止點
        start = split.getStart();
        end = start + split.getLength();
        is.seek(start);
        if (start != 0) {
            start += lr.readLine(new Text(), 0, (int) Math.min(Integer.MAX_VALUE, end - start));
        }
        currentPos = start;
        this.mode = conf.get(Tool.MODE, mode);
    }

    // 針對每行數據進行處理
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (currentPos > end) {
            return false;
        }
        currentPos += lr.readLine(line);
        if (line.getLength() == 0) {
            return false;
        }

        //input → 00c9ade79e49dc03ef57af6c13f851eca83ebb3f	DEBG2H-A70201693 DEBG2H  DEBG    5
        String[] data = line.toString().split(Tool.TAB);
        String value_info = data[1];    //商品推薦

        if (mode.equals("store")) {    //館推薦
            value_info = data[2] + Tool.UNDERLINE + value_info;
        } else if (mode.equals("region")) {   //區推薦
            value_info = data[3] + Tool.UNDERLINE + value_info;
        }

        key.set(data[0]);   // user id
        value.set(value_info);
        return true;

    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currentPos - start) / (float) (end - start));
        }
    }

    public void close() throws IOException {
        lr.close();
    }
}
