package jobUtil.combineSmallFile;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Joan on 2014/7/24.
 */
public class CustomCombineReader extends RecordReader<Text, Text> {
    private int index;
    private CustomReader in;

    public CustomCombineReader(CombineFileSplit split, TaskAttemptContext cxt, Integer index) {
        this.index = index;
        this.in = new CustomReader();
    }

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        CombineFileSplit cfsplit = (CombineFileSplit) split;
        FileSplit fileSplit = new FileSplit(cfsplit.getPath(index), cfsplit.getOffset(index), cfsplit.getLength(), cfsplit.getLocations());
        in.initialize(fileSplit, context);
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        return in.nextKeyValue();
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return in.getCurrentKey();
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
        return in.getCurrentValue();
    }

    public float getProgress() throws IOException, InterruptedException {
        return in.getProgress();
    }

    public void close() throws IOException {
        in.close();
    }
}
